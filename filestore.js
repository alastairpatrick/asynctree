const { createHash } = require("crypto");
const fs = require("fs");
const { dirname, join } = require("path");
const zlib = require("zlib");

const { promisify } = require("./promisify");
const { cloneNode, PTR } = require("./tree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const mkdir = promisify(fs.mkdir);
const readFile = promisify(fs.readFile);
const rename = promisify(fs.rename);
const unlink = promisify(fs.unlink);
const writeFile = promisify(fs.writeFile);

const deflate = promisify(zlib.deflate);
const unzip = promisify(zlib.unzip);


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
    }, config);

    this.cache = new Map();
    this.writing = new Map();
    this.syncPromise = Promise.resolve();
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
    
    return promise.then(text => {
      let node = JSON.parse(text);
      node[PTR] = ptr;
      this.cache_(node);
      return node;
    });
  }

  write(node) {
    let text = JSON.stringify(node);

    let hash = this.createHash();
    hash.update(text);
    let ptr = hash.digest("hex");
    ptr = ptr.substring(0, 2) + "/" + ptr.substring(2);

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
      this.syncPromise = Promise.all([new Promise(deleteFn), this.syncPromise]);
    }
  }

  cache_(node) {
    this.cache.delete(node);
    this.cache.set(node[PTR], node);

    for (let [ptr, node] of this.cache) {
      if (this.cache.size <= this.config.cacheSize)
        break;

      if (node[MUST_WRITE] !== undefined) {
        this.writeFile_(node);
      }

      this.cache.delete(ptr);
    }
  }

  writeFile_(node) {
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
      return writeFile(path, buffer).catch(error => {
        if (error.code !== "ENOENT")
          throw error;
        return mkdir(join(this.dir, dirname(ptr))).catch(error => {
          if (error.code !== "EEXIST")
            throw error;
        }).then(() => {
          return writeFile(path, buffer);
        });
      });
    }).then(() => {
      this.writing.delete(ptr);
    });
    node[MUST_WRITE] = undefined;
    this.writing.set(ptr, { node, promise });
  }

  flush() {
    for (let [ptr, node] of this.cache) {
      if (node[MUST_WRITE] !== undefined)
        this.writeFile_(node);
    }

    let promises = [this.syncPromise];
    for (let [ptr, nodePromise] of this.writing) {
      promises.push(nodePromise.promise);
    }

    this.cache.clear();
    this.writing.clear();
    this.syncPromise = Promise.resolve();

    return Promise.all(promises);
  }

  readIndexPtr() {
    let indexPath = join(this.dir, "index");
    return readFile(indexPath, { encoding: "utf-8" });
  }

  writeIndexPtr(ptr) {
    let tempPath = join(this.dir, "index.tmp");
    let indexPath = join(this.dir, "index");
    return writeFile(tempPath, ptr).then(() => {
      return rename(tempPath, indexPath);
    }).catch(error => {
      return unlink(tempPath).then(() => {;
        throw error;
      });
    });
  }
}

module.exports = {
  FileStore,
}
