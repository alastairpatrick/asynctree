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

const rmrf = (path) => {
  return lstat(path).catch(error => {
    if (error.code !== "ENOENT")
      throw error;
  }).then(stats => {
    if (stats === undefined) {
      return;
    } else if (stats.isDirectory()) {
      return readdir(path).then(entries => {
        let idx = -1;
        const processEntries = () => {
          if (++idx >= entries.length)
            return;
          let entry = entries[idx];
          return rmrf(join(path, entry)).then(processEntries);
        }
        return processEntries()
      }).then(() => {
        return rmdir(path);
      });
    } else {
      return unlink(path);
    }
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
      return rmrf(join(dir, "tmp"));
    }).then(() => {
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

  writeMeta(meta) {
    this.meta = meta;
  }

  flush() {
    for (let node of this.writes) {
      this.writeNodeFile_(node);
    }

    let promises = [];
    for (let task of this.pathTasks.values())
      promises.push(task.promise);
    
    return Promise.all(promises).then(() => {
      let text = JSON.stringify(this.meta);
      return this.writeFileAtomic_(this.metaPath(), text, { mode: this.config.fileMode });
    });
  }

  sync() {
    return this.flush().then(() => {
      this.cache.clear();
    });
  }
  
  cloneStore() {
    let newDir = this.tmpPath_();
    return FileStore.create(newDir, this.config).then(newStore => {
      return this.flush().then(() => {
        return readdir(join(this.dir, "node")).catch(error => {
          if (error.code !== "ENOENT")
            throw error;
          return [];
        });
      }).then(prefixes => {
        let prefixIdx = -1;
        const processNodes = () => {
          let promise = Promise.resolve();
          if (++prefixIdx >= prefixes.length)
            return promise;
          
          let file = prefixes[prefixIdx];
          if (/^[a-z0-9]{2}$/.test(file)) {
            promise = promise.then(() =>{
              return mkdirp(join(newDir, "node", file));
            }).then(() => {
              return readdir(join(this.dir, "node", file));
            }).then(nodes => {
              let nodeIdx = -1;
              const proocessSubDir = () => {
                if (++nodeIdx >= nodes.length)
                  return;
                
                let node = nodes[nodeIdx];
                return link(join(this.dir, "node", file, node), join(newDir, "node", file, node)).then(proocessSubDir);
              }

              return proocessSubDir();
            });
          }

          return promise.then(processNodes);
        }

        return processNodes().then(() => {
          newStore.writeMeta(this.meta);
          return newStore;
        });
      });
    });
  }

  renameStore(dir) {
    let oldDir = this.dir;
    this.dir = undefined;
    return rename(oldDir, dir).then(() => {
      this.dir = dir;
    });
  }

  deleteStore() {
    let dir = this.dir;
    this.invalidate_();
    return rmrf(dir);
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
