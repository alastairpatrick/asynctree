"use strict";

const { PTR, cloneNode } = require("..");

const has = Object.prototype.hasOwnProperty;


class TestStore {
  constructor() {
    this.nodes = {};
    this.pending = new Set();
    this.ptr = 1000;
    this.indexPtr = undefined;
  }
  
  read(ptr) {
    if (!has.call(this.nodes, ptr))
      return Promise.reject(new Error(`Pointer not found '${ptr}'.`));
    let node = cloneNode(this.nodes[ptr]);
    node[PTR] = ptr;
    return Promise.resolve(node);
  }

  beginWrite(node) {
    if (this.pending.has(node))
      throw new Error(`Already began writing '${ptr}'.`);
    this.pending.add(node);
    let ptr = ++this.ptr;
    node[PTR] = ptr;
  }

  endWrite(node) {
    if (!has.call(node, PTR))
      throw new Error(`Node '${node}' does not have a pointer.`);
    if (!this.pending.delete(node))
      throw new Error(`Did not begin writing '${node}'.`);
    let ptr = node[PTR];
    this.nodes[ptr] = cloneNode(node);
  }

  delete(ptr) {
    if (!has.call(this.nodes, ptr))
      return Promise.reject(new Error(`Pointer not found '${ptr}'.`));
    let node = this.nodes[ptr];
    if (this.pending.has(node))
      throw new Error(`Still writing '${ptr}'.`);
    delete this.nodes[ptr];
  }

  readIndexPtr() {
    return Promise.resolve(this.indexPtr);
  }

  writeIndexPtr(ptr) {
    this.indexPtr = ptr;
    return Promise.resolve();
  }

  flush() {
    return Promise.resolve();
  }

  check() {
    if (this.pending.size)
      throw new Error(`Pending writes: ${Array.from(this.pending.keys())}.`);
  }
}

module.exports = {
  TestStore,
};
