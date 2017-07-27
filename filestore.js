"use strict";

const { createHash } = require("crypto");
const fs = require("fs");
const os = require("os");
const { dirname, join, resolve } = require("path");
const zlib = require("zlib");

const { promisify } = require("./promisify");
const { PTR } = require("./tree");

const has = Object.prototype.hasOwnProperty;

const link = promisify(fs.link);
const lstat = promisify(fs.lstat);
const mkdtemp = promisify(fs.mkdtemp);
const mkdir = promisify(fs.mkdir);
const readdir = promisify(fs.readdir);
const readFile = promisify(fs.readFile);
const rename = promisify(fs.rename);
const rmdir = promisify(fs.rmdir);
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.writeFile);

const gzip = promisify(zlib.gzip);
const gunzip = promisify(zlib.gunzip);

const mkdirp = (path) => {
  return mkdir(path).catch(error => {
    if (error.code === "EEXIST")
      return;
    if (error.code !== "ENOENT")
      throw error;
    path = resolve(path);
    let parent = dirname(path);
    return mkdirp(parent).then(() => {
      return mkdir(path).catch(error => {
        if (error.code !== "EEXIST")
          throw error;
      });
    });
  });
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
  static create(dir, config) {
    return mkdirp(join(dir, "node")).then(() => {
      return mkdirp(join(dir, "tmp"));
    }).then(() => {
      return new FileStore(dir, config);
    });
  }

  constructor(dir, config) {
    config = Object.assign({
      cacheSize: 256,
      compress: true,
      fileMode: 0o644,
      maxConcurrentIO: 4,
      verifyHash: false,
    }, config);

    if ((config.fileMode & 0o600) !== 0o600)
      throw new Error("File mode must allow read and write access to user.");

    this.dir = dir;
    this.config = config;
    this.pathTasks = new Map();
    this.writes = new Set();
    this.cache = new Map();
    this.meta = undefined;
    this.tmpCount = 0;
  }
  
  invalidate_() {
    this.dir = undefined;
    this.config = undefined;
    this.pathTasks = undefined;
    this.writes = undefined;
    this.cache = undefined;
    this.meta = undefined;
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

  copy(fromStore, ptr, options) {
    options = Object.assign({
      tryLink: true,
      touch: false,
    }, options);

    let fromPath = fromStore.ptrPath_(ptr);
    let toPath = this.ptrPath_(ptr);

    const touch = () => {
      let tempPath = this.tmpPath_();
      return link(toPath, tempPath).then(() => {
        return unlink(tempPath);
      }).then(() => true);
    }

    const doCopy = () => {
      return readFile(fromPath).then(buf => {
        return writeFile(toPath, buf).then(() => true);
      });
    }

    const doLink = () => {
      if (options.touch) {
        let tempPath = this.tmpPath_();
        return link(fromPath, tempPath).then(() => {
          return rename(tempPath, toPath);
        }).then(() => true);
      } else {
        return link(fromPath, toPath).then(() => true).catch(error => {
          if (error.code !== "EEXIST")
            throw error;
          return false;
        });
      }
    }

    const retry = (fn) => {
      return fn().catch(error => {
        if (error.code !== "ENOENT")
          throw error;
        return mkdirp(dirname(toPath)).then(fn);
      });
    }

    return fromStore.flushWriteNodes().then(() => {
      return this.schedulePathTask_(toPath, () => {
        if (fromStore === this) {
          if (options.touch) {
            return touch();
          }
        } else {
          if (options.tryLink) {
            return retry(doLink).catch(() => {
              return retry(doCopy);
            });
          } else {
            return retry(doCopy);
          }
        }
      });
    });
  }

  metaPath() {
    return join(this.dir, "meta");
  }

  readMeta() {
    if (this.meta !== undefined)
      return Promise.resolve(this.meta);
    return readFile(this.metaPath(), { encoding: "utf-8" }).then(text => {
      return this.meta = JSON.parse(text);
    });
  }

  writeMeta(transform) {
    if (typeof transform !== "function") {
      let data = transform;
      transform = () => data;
    }

    let metaPath = this.metaPath();
    let lockPath = metaPath + ".lock";

    let tries = 0;
    const tryWrite = () => {
      if (tries >= 100)
        throw new Error("Tried to lock meta file too many times");
      ++tries;

      // This is synchonous so other threads do not cause the lock file to be abandoned.
      let fd;
      let unlinkPath;
      try {
        fd = fs.openSync(lockPath, "wx", this.config.fileMode);
        unlinkPath = lockPath;
      } catch (error) {
        if (error.code !== "EEXIST")
          throw error;
        return Promise.resolve().then(tryWrite);
      }

      try {
        let text;
        try {
          text = fs.readFileSync(metaPath, { encoding: "utf-8" });
        } catch (error) {
          if (error.code !== "ENOENT")
            throw error;
          text = "{}";
        }

        let meta = JSON.parse(text);
        let newMeta = transform(meta);
        if (newMeta !== undefined)
          meta = newMeta;
        text = JSON.stringify(meta);
        fs.writeFileSync(fd, text);
        fs.closeSync(fd);
        fd = undefined;

        fs.renameSync(lockPath, metaPath);
        unlinkPath = undefined;

        return meta;
      } finally {
        if (fd !== undefined)
          fs.closeSync(fd);
        if (unlinkPath !== undefined)
          fs.unlinkSync(unlinkPath);
      }
    }

    return this.flushWriteNodes().then(tryWrite);
  }

  flushWriteNodes() {
    for (let node of this.writes)
      this.writeNodeFile_(node);

    let promises = [];
    for (let task of this.pathTasks.values())
      promises.push(task.promise);
    
    return Promise.all(promises);
  }

  flush() {
    return this.flushWriteNodes();
  }

  sync() {
    return this.flush().then(() => {
      this.cache.clear();
    });
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

  ptrPath_(ptr) {
    let path;
    if (typeof ptr === "string")
      path = ptr;
    else
      path = ptr.toJSON();
    path = join(this.dir, "node", path);
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
    this.writes.delete(node);

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
        return this.writeFileAtomic_(path, buffer, { mode: this.config.fileMode });
      });
    });
  }

  writeFileAtomic_(path, data, options) {
    options = Object.assign({}, options, { flags: "wx" });

    let tmpPath = this.tmpPath_();
    return writeFile(tmpPath, data, options).then(() => {
      return rename(tmpPath, path).catch(error => {
        if (error.code !== "ENOENT")
          throw error;
        return mkdirp(dirname(path)).then(() => {
          return rename(tmpPath, path);
        });
      }).catch(error => {
        return unlink(tmpPath).catch(() => {
          throw error;
        }).then(() => {
          throw error;
        });
      });
    });
  }

  tmpPath_() {
    let tmpPath = join(this.dir, "tmp", this.tmpCount.toString(36));
    ++this.tmpCount;
    return tmpPath;
  }
}

module.exports = {
  FileStore,
}
