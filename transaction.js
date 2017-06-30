"use strict"

const { PTR } = require("./base");

class Transaction {
  constructor(store, parent) {
    if (parent) {
      if (store !== parent.store)
        throw new Error("Store mismatch");
      ++parent.children;
    }

    this.store = store;
    this.parent = parent;
    this.undos = new Set();
    this.applies = new Set();
    this.children = 0;
  }

  read(ptr) {
    return this.store.read(ptr);
  }

  beginWrite(node) {
    this.store.beginWrite(node);
  }

  endWrite(node) {
    this.store.endWrite(node);
    this.undos.add(node[PTR]);
  }

  delete(ptr) {
    if (this.undos.delete(ptr))
      this.store.delete(ptr);
    else
      this.applies.add(ptr);
  }

  rollback() {
    if (this.undos === undefined)
      throw new Error("Transaction already ended.");

    if (this.children)
      throw new Error(`${this.children} child transactions must first commit or rollback.`);

    if (this.parent)
      --this.parent.children;

    for (let ptr of this.undos)
      this.store.delete(ptr);

    this.undos = undefined;
    this.applies = undefined;
  }

  commit() {
    if (this.undos === undefined)
      throw new Error("Transaction already ended.");

    if (this.children)
      throw new Error(`${this.children} child transactions must first commit or rollback.`);

    if (this.parent) {
      --this.parent.children;

      for (let ptr of this.undos)
        this.parent.undos.add(ptr);
      for (let ptr of this.applies)
        this.parent.delete(ptr);
    }

    this.undos = undefined;
    this.applies = undefined;
  }
}

module.exports = {
  Transaction,
};
