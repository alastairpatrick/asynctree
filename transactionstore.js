"use strict"

const { PTR } = require("./base");

class TransactionStore {
  constructor(store, rootPtr) {
    this.parent = store;
    this.rootPtr = rootPtr;
    this.undos = new Set();
    this.applies = new Set();
  }

  read(ptr) {
    return this.parent.read(ptr);
  }

  write(node) {
    this.parent.write(node);
    let ptr = node[PTR];
    this.applies.delete(ptr);
    this.undos.add(ptr);
  }

  delete(ptr) {
    if (this.undos.delete(ptr))
      this.parent.delete(ptr);
    else if (this.parent instanceof TransactionStore)
      this.applies.add(ptr);
  }

  rollback() {
    for (let ptr of this.undos)
      this.parent.delete(ptr);

    this.undos = new Set();
    this.applies = new Set();

    return this.rootPtr;
  }

  commit(rootPtr) {
    for (let ptr of this.applies)
      this.parent.delete(ptr);

    this.rootPtr = rootPtr;
    this.undos = new Set();
    this.applies = new Set();
  }
}

module.exports = {
  TransactionStore,
};
