"use strict";

const { PTR, cloneNode } = require("..");

const has = Object.prototype.hasOwnProperty;


class TestStore {
  constructor() {
    this.nodes = {};
    this.ptr = 1000;
    this.meta = undefined;
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

  copy(fromStore, ptr) {
    return fromStore.read(ptr).then(node => {
      this.nodes[ptr] = cloneNode(node);
      this.ptr = Math.max(this.ptr, ptr + 1);
      return true;
    });
  }

  readMeta() {
    if (this.meta === undefined)
      return Promise.reject(new Error("Not found"));
    return Promise.resolve(this.meta);
  }

  writeMeta(path, meta) {
    this.meta = meta;
  }

  flush() {
    return Promise.resolve();
  }
}

module.exports = {
  TestStore,
};
