"use strict";

const { PTR } = require("..");

const has = Object.prototype.hasOwnProperty;


class TestStore {
  constructor() {
    this.nodes = {};
    this.ptr = 1000;
    this.indexPtr = undefined;
  }
  
  read(ptr) {
    if (!has.call(this.nodes, ptr))
      return Promise.reject(new Error(`Pointer not found '${ptr}'.`));
    let node = this.nodes[ptr];
    node[PTR] = ptr;
    return Promise.resolve(node);
  }

  write(node) {
    let ptr = this.ptr++;
    node[PTR] = ptr;
    this.nodes[ptr] = node;
  }

  delete(ptr) {
    if (!has.call(this.nodes, ptr))
      return Promise.reject(new Error(`Pointer not found '${ptr}'.`));
    let node = this.nodes[ptr];
    delete this.nodes[ptr];
  }

  readMeta(path) {
    return Promise.resolve(this.index);
  }

  writeMeta(path, index) {
    this.index = index;
    return Promise.resolve();
  }

  flush() {
    return Promise.resolve();
  }
}

module.exports = {
  TestStore,
};
