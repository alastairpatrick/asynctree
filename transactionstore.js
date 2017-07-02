"use strict"

const { PTR } = require("./base");

class TransactionStore {
  constructor(store) {
    this.store = store;
    this.undos = new Set();
    this.applies = new Set();
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
  }

  rollback() {
    for (let ptr of this.undos)
      this.store.delete(ptr);

    this.undos = new Set();
  }

  commit() {
    this.undos = new Set();
  }
}

module.exports = {
  TransactionStore,
};
