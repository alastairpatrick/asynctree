const { mkdir, mkdtemp, readFile, rename, unlink, writeFile } = require("fs");
const { join, relative, sep } = require("path");

const { cloneNode, PTR } = require("./tree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const toString36 = (n) => {
  return ("000000" + n.toString(36)).slice(-6);
}

const makeDir = (dir) => {
  return new Promise((resolve, reject) => {
    mkdir(dir, (error) => {
      if (error)
        reject(error);
      else
        resolve();
    });
  })
}

class FileStore {
  constructor(dir, sessionName) {
    this.dir = dir;
    this.sessionName = sessionName;
    this.nodeIdx = 0;
    this.cache = new Map();
    this.writing = new Map();
    this.syncPromise = Promise.resolve();
    this.cacheSize = 12;
  }

  nextPtr() {
    return this.sessionName + "/" + toString36(this.nodeIdx++);
  }

  static newSession(dir) {
    return new Promise((resolve, reject) => {
      mkdtemp(dir + sep, (error, dir) => {
        if (error)
          reject(error);
        else
          resolve(dir);
      });
    }).then(sessionDir => {
      let sessionName = relative(dir, sessionDir);
      let store = new FileStore(dir, sessionName);
      return store;
    });
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
    return new Promise((resolve, reject) => {
      readFile(path, (error, data) => {
        if (error)
          reject(error);
        else
          resolve(data);
      });
    }).then(data => {
      let node = JSON.parse(data);
      node[PTR] = ptr;
      this.cache_(node);
      return node;
    });
  }

  write(node) {
    node[PTR] = this.nextPtr();
    node[MUST_WRITE] = true;
    this.cache_(node);
  }

  delete(ptr) {
    // The most commomn case: if the node was never written to file then only need to delete from cache.
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache.delete(ptr);
      if (node[MUST_WRITE])
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
      if (this.cache.size <= this.cacheSize)
        break;

      if (node[MUST_WRITE]) {
        this.writeFile_(node);
      }

      this.cache.delete(ptr);
    }
  }

  writeFile_(node) {
    let ptr = node[PTR];
    let path = join(this.dir, ptr);
    let promise = new Promise((resolve, reject) => {
      writeFile(path, JSON.stringify(node), (error) => {
        if (error)
          reject(error);
        else
          resolve();
      });
    }).then(() => {
      this.writing.delete(ptr);
    });
    node[MUST_WRITE] = false;
    this.writing.set(ptr, { node, promise });
  }

  flush() {
    for (let [ptr, node] of this.cache) {
      if (node[MUST_WRITE])
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
    return new Promise((resolve, reject) => {
      readFile(indexPath, { encoding: "utf-8" }, (error, data) => {
        if (error)
          reject(error);
        else
          resolve(data);
      })
    });
  }

  writeIndexPtr(ptr) {
    let tempPath = join(this.dir, this.nextPtr());
    let indexPath = join(this.dir, "index");
    return new Promise((resolve, reject) => {
      writeFile(tempPath, ptr, error => {
        if (error)
          reject(error);
        else
          resolve();
      });
    }).then(() => {
      return new Promise((resolve, reject) => {
        rename(tempPath, indexPath, error => {
          if (error)
            reject(error);
          else
            resolve();
        });
      }).catch(error => {
        unlink(tempPath, error => {});
        throw error;
      });
    });
  }
}

module.exports = {
  FileStore,
}
