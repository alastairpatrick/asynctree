const { mkdir, mkdtemp, readFile, writeFile } = require("fs");
const { join, relative, sep } = require("path");

const has = Object.prototype.hasOwnProperty;

const toString36 = (n) => {
  return ("000000" + n.toString(36)).slice(-6);
}

class FileStore {
  constructor(dir, sessionName) {
    this.dir = dir;
    this.sessionName = sessionName;
    this.commitIdx = 0;
    this.nodeIdx = 0;
    this.nodes = {};
  }

  nextPtr() {
    return this.sessionName + "/" + toString36(this.commitIdx) + "/" + toString36(this.nodeIdx++);
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
      return new FileStore(dir, sessionName);
    });
  }

  read(ptr) {
    if (has.call(this.nodes, ptr))
      return Promise.resolve(this.nodes[ptr]);

    let path = join(this.dir, ptr);
    return new Promise((resolve, reject) => {
      readFile(path, (error, data) => {
        if (error)
          reject(error);
        else
          resolve(data);
      });
    }).then(data => {
      return JSON.parse(data);
    });
  }

  write(node) {
    let ptr = this.nextPtr();
    this.nodes[ptr] = node;
    return ptr;
  }

  commit(tree) {
    let promises = [];
    return new Promise((resolve, reject) => {
      mkdir(join(this.dir, this.sessionName, toString36(this.commitIdx)), (error) => {
        if (error)
          reject(error);
        else
          resolve();
      });
    }).then(() => {
      return tree.mark(ptr => {
        if (!has.call(this.nodes, ptr))
          return false;
        let node = this.nodes[ptr];
        delete this.nodes[ptr];

        let path = join(this.dir, ptr);
        promises.push(new Promise((resolve, reject) => {
          writeFile(path, JSON.stringify(node), (error) => {
            if (error)
              reject(error);
            else
              resolve();
          })
        }));
      });
    }).then(() => {
      return Promise.all(promises).then(() => {
        ++this.commitIdx;
        this.nodeIdx = 0;
        this.nodes = {};
      });
    });
  }

  rollback() {
    this.nodeIdx = 0;
    this.nodes = {};
  }
}

module.exports = {
  FileStore,
}
