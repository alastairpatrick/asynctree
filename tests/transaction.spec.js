"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { PTR, Transaction, cloneNode } = require("..");

const has = Object.prototype.hasOwnProperty;

class TestStore {
  constructor() {
    this.nodes = {};
    this.pending = new Set();
    this.ptr = 1000;
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

  check() {
    if (this.pending.size)
      throw new Error(`Pending writes: ${Array.from(this.pending.keys())}.`);
  }
}

describe("Transaction", function() {
  beforeEach(function() {
    this.store = new TestStore();
    this.transaction = new Transaction(this.store);
  })

  it("reads store", function() {
    this.store.nodes[1] = { keys: [1], values: [1] };
    this.transaction.read(1).then(value => {
      expect(value).to.deep.equal(this.store.nodes[1]);
    });
  })

  it("rolls back newly written node", function() {
    let node = { keys: [1], values: [1] };
    this.transaction.beginWrite(node);
    let ptr = node[PTR];
    this.transaction.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    this.transaction.rollback();
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  it("deletes node immediately if it was written in current transaction", function() {
    let node = { keys: [1], values: [1] };
    this.transaction.beginWrite(node);
    let ptr = node[PTR];
    this.transaction.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);

    this.transaction.delete(ptr);
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  it("does not delete nodes not written within current transaction", function() {
    let node = { keys: [1], values: [1] };    
    this.store.nodes[1] = node;
    this.transaction.delete(1);
    expect(this.store.nodes[1]).to.deep.equal(node);
    this.transaction.commit();
    expect(this.store.nodes[1]).to.deep.equal(node);
  })

  it("may not commit ended transaction", function() {
    this.transaction.commit();
    expect(() => {
      this.transaction.commit();
    }).to.throw(/ended/);
  })

  it("may not rollback ended transaction", function() {
    this.transaction.rollback();
    expect(() => {
      this.transaction.rollback();
    }).to.throw(/ended/);
  })

  describe("nested", function() {
    beforeEach(function() {
      this.childTransaction = new Transaction(this.store, this.transaction);
    })

    it("increases children count of parent", function() {
      expect(this.transaction.children).to.equal(1);
      expect(this.childTransaction.children).to.equal(0);
    })

    it("committing ends child transaction", function() {
      this.childTransaction.commit();
      expect(this.transaction.children).to.equal(0);
      this.transaction.commit();
    })

    it("rolling back ends child transaction", function() {
      this.childTransaction.rollback();
      expect(this.transaction.children).to.equal(0);
      this.transaction.rollback();
    })

    it("error on attempt to commit parent transaction before child", function() {
      expect(() => {
        this.transaction.rollback();
      }).to.throw(/child transaction/);
    })

    it("error on attempt to rollback parent transaction before child", function() {
      expect(() => {
        this.transaction.rollback();
      }).to.throw(/child transaction/);
    })

    it("deletes nodes written by parent transaction when commiting child transaction", function() {
      let node = { keys: [1], values: [1] };
      this.transaction.beginWrite(node);
      let ptr = node[PTR];
      this.transaction.endWrite(node);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTransaction.delete(ptr);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTransaction.commit();
      expect(this.store.nodes[ptr]).to.be.undefined;
    })

    it("transaction must use same store as parent", function() {
      expect(() => {
        new Transaction(new TestStore(), this.transaction);
      }).to.throw(/mismatch/);
    })

  })
})
