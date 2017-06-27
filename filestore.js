const { mkdir, mkdtemp, readdir, readFile, unlink, writeFile } = require("fs");
const { join, relative, sep } = require("path");

const { cloneNode, PTR } = require("./asynctree");

const MUST_WRITE = Symbol("MUST_WRITE");

const has = Object.prototype.hasOwnProperty;

const toString36 = (n) => {
  return ("000000" + n.toString(36)).slice(-6);
}

const transactionDir = (dir, sessionName, transaction) => {
  return join(dir, sessionName, toString36(transaction));
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
    this.transaction = 0;
    this.transactionPrefix = this.sessionName + "/" + toString36(this.transaction);
    this.nodeIdx = 0;
    this.cache = new Map();
    this.writing = new Map();
    this.transactionPromise = Promise.resolve();
    this.cacheSize = 12;
  }

  nextPtr() {
    return this.transactionPrefix + "/" + toString36(this.nodeIdx++);
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
      return makeDir(transactionDir(dir, sessionName, store.transaction)).then(() => store);
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

  beginWrite(node) {
    let ptr = this.nextPtr();
    node[PTR] = ptr;
  }

  endWrite(node) {
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

    // The tree will delete nodes from prior transactions. It is the responsibility of the store to delete these
    // nodes from its transaction local cache but not from storage visible to other transactions, i.e. the file system.
    if (ptr.substring(0, this.transactionPrefix.length) !== this.transactionPrefix)
      return;

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
      this.transactionPromise = Promise.all([new Promise(deleteFn), this.transactionPromise]);
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

  commit(tree) {
    for (let [ptr, node] of this.cache) {
      if (node[MUST_WRITE])
        this.writeFile_(node);
    }

    let promises = [this.transactionPromise];
    for (let [ptr, nodePromise] of this.writing) {
      promises.push(nodePromise.promise);
    }

    ++this.transaction;
    this.transactionPrefix = this.sessionName + "/" + toString36(this.transaction);
    this.nodeIdx = 0;
    this.cache = new Map();
    this.writing = new Map();
    this.transactionPromise = Promise.resolve();
    promises.push(makeDir(transactionDir(this.dir, this.sessionName, this.transaction)));

    return Promise.all(promises);
  }

  rollback() {
    let promises = [this.transactionPromise];
    for (let [ptr, nodePromise] of this.writing) {
      promises.push(nodePromise.promise);
    }
    return Promise.all(promises).then(() => {
      let dir = transactionDir(this.dir, this.sessionName, this.transaction);

      this.nodeIdx = 0;
      this.cache = new Map();
      this.writing = new Map();
      this.transactionPromise = Promise.resolve();

      return new Promise((resolve, reject) => {
        readdir(dir, (error, files) => {
          if (error)
            reject(error);
          else
            resolve(files);
        });
      }).then(files => {
        let promises = [];
        files.forEach(file => {
          promises.push(new Promise((resolve, reject) => {
            unlink(join(dir, file), (error) => {
              if (error)
                reject(error);
              else
                resolve();
            });
          }));
        });
        return Promise.all(promises);
      })
    });
  }
}

module.exports = {
  FileStore,
}
