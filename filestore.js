"use strict";

const { createHash } = require("crypto");
const fs = require("fs");
const os = require("os");
const { dirname, join } = require("path");
const zlib = require("zlib");

const { promisify } = require("./promisify");
const { PTR } = require("./tree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const mkdir = promisify(fs.mkdir);
const readFile = promisify(fs.readFile);
const rename = promisify(fs.rename);
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.writeFile);

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

const tempDir = fs.mkdtempSync(join(os.tmpdir(), "filestore"));
let tempCount = 0;

const cleanup = () => {
  let files = fs.readdirSync(tempDir);
  files.forEach(file => {
    console.error(`Deleting temporary file '${file}'.`);
    fs.unlinkSync(join(tempDir, file));
  });
  fs.rmdirSync(tempDir);
}

process.on("exit", cleanup);
process.on("SIGINT", cleanup);

const ensureDir = (dir) => {
  return mkdir(dir).catch(error => {
    if (error.code !== "EEXIST")
      throw error;
  })
}

class Ptr {
  constructor(store, node) {
    this.store = store;
    this.node = node;
    this.hash = undefined;
  }

  build(text) {
    if (this.hash === undefined);
      this.hash = this.store.hash_(text);
  }

  toJSON() {
    if (this.hash !== undefined)
      return this.hash;
    
    if (this.node === undefined)
      this.hash = null;
    else
      this.build(JSON.stringify(this.node));
    return this.hash;
  }
}

class FileStore {
  constructor(dir, config) {
    this.dir = dir;
    this.config = Object.assign({
      cacheSize: 256,
      compress: true,
      fileMode: 0o444,
      maxConcurrentIO: 4,
      verifyHash: false,
    }, config);

    this.pathTasks = new Map();
    this.writes = new Set();
    this.cache = new Map();
  }
  
  createHasher() {
    // Not intended to be resilient to deliberate collisions attempts. When that's an issue, override
    // this function to use another hash function or HMAC.
    return createHash("md5");
  }

  read(ptr) {
    // Wait until the number of IO operations falls below the limit so we don't exceed the system's file
    // handle limit.
    const throttle = () => {
      if (this.pathTasks.size <= this.config.maxConcurrentIO)
        return Promise.resolve();

      let promises = [];
      for (let pathTask of this.pathTasks.values())
        promises.push(pathTask.promise);
        
      return Promise.race(promises).then(throttle);
    }

    return throttle().then(() => {
      if (typeof ptr !== "string") {
        if (ptr.node !== undefined) {
          return ptr.node;
        } else {
          if (ptr.hash === undefined)
            throw new Error("Pointer was deleted");
          else
            ptr = ptr.hash;
        }
      }
      
      let node = this.cache.get(ptr);
      if (node !== undefined)
        return node;

      let path = this.ptrPath_(ptr);
      return this.schedulePathTask_(path, () => {
        let promise = readFile(path);
        
        if (this.config.compress)
          promise = promise.then(gunzip);
      
        if (this.config.verifyHash) {
          promise = promise.then(text => {
            let hash = this.hash_(text);
            if (String(ptr) !== hash)
              throw new Error(`Data for node '${ptr}' does not match hash digest.`);
            return text;
          });
        }

        return promise.then(text => {
          let node = JSON.parse(text);
          node[PTR] = ptr;

          this.cache.delete(ptr);
          this.cache_(ptr, node);
          return node;
        }).catch(error => {
          throw error;
        });
      });
    });
  }
  
  write(node) {
    let ptr = new Ptr(this, node);
    node[PTR] = ptr;
    this.writes.add(node);

    for (let evictNode of this.writes) {
      if (this.writes.size <= this.config.cacheSize)
        break;
      this.writeNodeFile_(evictNode);
      this.writes.delete(evictNode);
    }
  }
  
  delete(ptr) {
    if (typeof ptr === "string")
      return Promise.reject(new Error("Cannot delete node that was not written by this store."));

    if (!this.writes.delete(ptr.node)) {
      let path = this.ptrPath_(ptr);
      this.schedulePathTask_(path, () => {
        return unlink(path);
      });
    }

    ptr.node = undefined;
    ptr.hash = undefined;
  }
  
  schedulePathTask_(path, task) {
    let pathTask = this.pathTasks.get(path);
    if (pathTask === undefined) {
      pathTask = {
        count: 0,
        promise: Promise.resolve(),
      };
      this.pathTasks.set(path, pathTask);
    }
    ++pathTask.count;
    return pathTask.promise = pathTask.promise.then(task).catch(error => {
      if (--pathTask.count === 0)
        this.pathTasks.delete(path);
      throw error;
    }).then(result => {
      if (--pathTask.count === 0)
        this.pathTasks.delete(path);
      return result;
    });
  }


  flush() {
    for (let node of this.writes) {
      this.writeNodeFile_(node);
    }

    let promises = [];
    for (let task of this.pathTasks.values())
      promises.push(task.promise);
    
    return Promise.all(promises);
  }

  sync() {
    return this.flush().then(() => {
      this.cache.clear();
    });
  }

  readMeta(path) {
    let indexPath = join(this.dir, path);
    return readFile(indexPath, { encoding: "utf-8" }).then(JSON.parse).catch(error => {
      if (error.code !== "ENOENT")
        throw error;
    });
  }

  writeMeta(path, index) {
    let indexPath = join(this.dir, path);
    return this.writeFileAtomic_(indexPath, JSON.stringify(index), { mode: this.config.fileMode });
  }

  ptrPath_(ptr) {
    let path;
    if (typeof ptr === "string")
      path = ptr;
    else
      path = ptr.toJSON();
    path = join(this.dir, path);
    if (this.config.compress)
      path += ".gz";
    return path;
  }

  hash_(text) {
    let hasher = this.createHasher();
    hasher.update(text);
    let hash = hasher.digest("hex");
    hash = hash.substring(0, 2) + "/" + hash.substring(2);
    return hash;
  }

  cache_(ptr, node) {
    this.cache.set(ptr, node);
    for (let evictPtr of this.cache.keys()) {
      if (this.cache.size <= this.config.cacheSize)
        break;
      this.cache.delete(evictPtr);
    }
  }

  writeNodeFile_(node) {
    let text = JSON.stringify(node);
    node[PTR].build(text);
    let path = this.ptrPath_(node[PTR]);
    node[PTR].node = undefined;
    this.cache_(node[PTR].hash, node);

    this.schedulePathTask_(path, () => {
      let promise;
      if (this.config.compress) {
        promise = gzip(text);
      } else {
        promise = Promise.resolve(text);
      }
      
      return promise.then(buffer => {
        return this.writeFileAtomic_(path, buffer, { mode: this.config.fileMode }).catch(error => {
          if (error.code !== "ENOENT")
            throw error;
          return mkdir(dirname(path)).catch(error => {
            if (error.code !== "EEXIST")
              throw error;
          }).then(() => {
            return this.writeFileAtomic_(path, buffer, { mode: this.config.fileMode })
          });
        });
      });
    });
  }

  writeFileAtomic_(path, data, options) {
    options = Object.assign({}, options, { flags: "wx" });

    let tempPath = join(tempDir, tempCount.toString(36) + "." + Math.random().toString(36).substring(2));
    ++tempCount;
    return writeFile(tempPath, data, options).then(() => {
      return rename(tempPath, path).catch(error => {
        return unlink(tempPath).catch(() => {
          if (error.code !== "EPERM")
            throw error;
        }).then(() => {
          // Windows doesn't allow one file to be renamed over another; instead its an error. Can ignore the
          // error because the target file had the same hash and therefore content as the file intended as its
          // replacement.
          if (error.code !== "EPERM")
            throw error;
        });
      });
    });
  }
}

module.exports = {
  FileStore,
}
