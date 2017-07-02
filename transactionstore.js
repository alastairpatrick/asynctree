"use strict"

const { PTR } = require("./base");

class TransactionStore {
  constructor(store) {
    this.parent = store;
    this.undos = new Set();
    this.applies = new Set();
  }

  read(ptr) {
    return this.parent.read(ptr);
  }

  beginWrite(node) {
    this.parent.beginWrite(node);
  }

  endWrite(node) {
    this.parent.endWrite(node);
    this.undos.add(node[PTR]);
  }

  delete(ptr) {
    if (this.undos.delete(ptr))
      this.parent.delete(ptr);
    else
      this.applies.add(ptr);
  }

  rollback() {
    for (let ptr of this.undos)
      this.parent.delete(ptr);

    this.undos = new Set();
    this.applies = new Set();
  }

  commit() {
    if (this.parent instanceof TransactionStore) {
      for (let ptr of this.undos)
        this.parent.undos.add(ptr);
      for (let ptr of this.applies)
        this.parent.delete(ptr);
    }

    this.undos = new Set();
  }
}

module.exports = {
  TransactionStore,
};
