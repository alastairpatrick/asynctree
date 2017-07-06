const { createHash } = require("crypto");
const fs = require("fs");
const os = require("os");
const { dirname, join } = require("path");
const zlib = require("zlib");

const { promisify } = require("./promisify");
const { cloneNode, PTR } = require("./tree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const close = promisify(fs.close);
const fsync = promisify(fs.fsync);
const mkdir = promisify(fs.mkdir);
const open = promisify(fs.open);
const readFile = promisify(fs.readFile);
const rename = promisify(fs.rename);
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.writeFile);

const deflate = promisify(zlib.deflate);
const unzip = promisify(zlib.unzip);

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
process.on("uncaughtException", cleanup);

const ensureDir = (dir) => {
  return mkdir(dir).catch(error => {
    if (error.code !== "EEXIST")
      throw error;
  })
}

class FileStore {
  constructor(dir, config={}) {
    this.dir = dir;
    this.config = Object.assign({
      cacheSize: 12,
      compress: true,
      fileMode: 0o444,
      maxPendingSyncs: 100,
      verifyHash: false,
    }, config);

    this.cache = new Map();
    this.writing = new Map();
    this.syncPromises = [];
  }

  createHash() {
    // Not intended to be resilient to deliberate collisions attempts. When that's an issue, override
    // this function to use another hash function or HMAC.
    return createHash("md5");
  }

  read(ptr) {
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache_(node);
      return Promise.resolve(cloneNode(node));      
    }

    let nodePromise = this.writing.get(ptr);
    if (nodePromise !== undefined)
      return Promise.resolve(cloneNode(nodePromise.node));      

    let path = join(this.dir, ptr);

    if (this.config.compress)
      path += ".gz";

    let promise = readFile(path);
    
    if (this.config.compress)
      promise = promise.then(unzip);
  
    if (this.config.verifyHash) {
      promise = promise.then(text => {
        let hash = this.hash_(text);
        if (ptr !== hash)
          throw new Error(`Data for node '${ptr}' is does not match hash digest.`);
        return text;
      });
    }

    return promise.then(text => {
      let node = JSON.parse(text);
      node[PTR] = ptr;
      this.cache_(node);
      return node;
    });
  }

  write(node) {
    let text = JSON.stringify(node);
    let ptr = this.hash_(text);
    node[MUST_WRITE] = text;
    node[PTR] = ptr;
    this.cache_(node);
  }

  delete(ptr) {
    // The most commomn case: if the node was never written to file then only need to delete from cache.
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache.delete(ptr);
      if (node[MUST_WRITE] !== undefined)
        return;
    }

    // If the node is in the process of writing to a file, chase the write with a delete. Otherwise, delete immediately.
    let path = join(this.dir, ptr);
    let deleteFn = (resolve, reject) => {
      unlink(path, (error) => {
        if (error)
          reject(error);
        else
          resolve();
      });
    };

    let nodePromise = this.writing.get(ptr);
    if (nodePromise !== undefined) {
      nodePromise.node = undefined;
      nodePromise.promise = nodePromise.promise.then(() => new Promise(deleteFn));
    } else {
      this.syncPromises.push(new Promise(deleteFn));
    }
  }

  hash_(text) {
    let hash = this.createHash();
    hash.update(text);
    let ptr = hash.digest("hex");
    ptr = ptr.substring(0, 2) + "/" + ptr.substring(2);
    return ptr;
  }

  cache_(node) {
    this.cache.delete(node);
    this.cache.set(node[PTR], node);

    for (let [ptr, node] of this.cache) {
      if (this.cache.size <= this.config.cacheSize)
        break;

      if (node[MUST_WRITE] !== undefined) {
        this.writeNodeFile_(node);
      }

      this.cache.delete(ptr);
    }
  }

  writeNodeFile_(node) {
    let ptr = node[PTR];
    let path = join(this.dir, ptr);
    let text = node[MUST_WRITE];
    let promise;
    if (this.config.compress) {
      path += ".gz";
      promise = deflate(text);
    } else {
      promise = Promise.resolve(text);
    }
    promise = promise.then(buffer => {
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
    }).then(() => {
      this.writing.delete(ptr);
    });
    node[MUST_WRITE] = undefined;
    this.writing.set(ptr, { node, promise });
  }

  writeFileAtomic_(path, data, options) {
    options = Object.assign({}, options, { flags: "wx" });

    let tempPath = join(tempDir, tempCount.toString(36) + "." + Math.random().toString(36).substring(2));
    ++tempCount;
    return open(tempPath, "wx", options.mode).then(fd => {
      return writeFile(fd, data, options).then(() => {
        return this.queueSync_(fsync(fd));
      }).catch(error => {
        return close(fd).then(() => {
          throw error;
        });
      }).then(() => {
        return close(fd)
      });
    }).then(() => {
      return rename(tempPath, path).catch(error => {
        return unlink(tempPath).catch(() => {
          throw error;
        }).then(() => {
          throw error;
        });
      });
    });
  }

  queueSync_(promise) {
    this.syncPromises.push(promise);
    if (this.syncPromises.length > this.config.maxPendingSyncs)
      return Promise.all(this.syncPromises.splice(0, this.syncPromises.length - this.config.maxPendingSyncs));
    else
      return Promise.resolve();
  }

  flush() {
    for (let [ptr, node] of this.cache) {
      if (node[MUST_WRITE] !== undefined)
        this.writeNodeFile_(node);
    }

    let promises = this.syncPromises;
    this.syncPromises = [];
    for (let [ptr, nodePromise] of this.writing) {
      promises.push(nodePromise.promise);
    }

    this.cache.clear();
    this.writing.clear();

    return Promise.all(promises);
  }

  readIndexPtr() {
    let indexPath = join(this.dir, "index");
    return readFile(indexPath, { encoding: "utf-8" });
  }

  writeIndexPtr(ptr) {
    let indexPath = join(this.dir, "index");
    return this.writeFileAtomic_(indexPath, ptr, { mode: this.config.fileMode });
  }
}

module.exports = {
  FileStore,
}
