"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { PTR, TransactionStore, cloneNode } = require("..");

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

describe("TransactionStore", function() {
  beforeEach(function() {
    this.store = new TestStore();
    this.tx = new TransactionStore(this.store);
  })

  it("reads store", function() {
    this.store.nodes[1] = { keys: [1], values: [1] };
    this.tx.read(1).then(value => {
      expect(value).to.deep.equal(this.store.nodes[1]);
    });
  })

  it("rolls back newly written node", function() {
    let node = { keys: [1], values: [1] };
    this.tx.beginWrite(node);
    let ptr = node[PTR];
    this.tx.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    this.tx.rollback();
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  it("deletes node immediately if it was written in current transaction", function() {
    let node = { keys: [1], values: [1] };
    this.tx.beginWrite(node);
    let ptr = node[PTR];
    this.tx.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);

    this.tx.delete(ptr);
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  it("does not delete nodes not written within current transaction", function() {
    let node = { keys: [1], values: [1] };    
    this.store.nodes[1] = node;
    this.tx.delete(1);
    expect(this.store.nodes[1]).to.deep.equal(node);
    this.tx.commit();
    expect(this.store.nodes[1]).to.deep.equal(node);
  })

  it("only rolls back changes since last commit", function() {
    let node1 = { keys: [1], values: [1] };
    this.tx.beginWrite(node1);
    let ptr1 = node1[PTR];
    this.tx.endWrite(node1);
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);

    this.tx.commit();

    let node2 = { keys: [2], values: [2] };
    this.tx.beginWrite(node2);
    let ptr2 = node2[PTR];
    this.tx.endWrite(node2);
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.deep.equal(node2);

    this.tx.rollback();
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.be.undefined;
  })

  it("may not rollback twice", function() {
    let node = { keys: [1], values: [1] };
    this.tx.beginWrite(node);
    let ptr = node[PTR];
    this.tx.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    this.tx.rollback();
    this.tx.rollback();
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  describe("nested", function() {
    beforeEach(function() {
      this.childTx = new TransactionStore(this.tx);
    })

    it("can commit child then parent transaction", function() {
      this.childTx.commit();
      this.tx.commit();
    })

    it("can rollback child then parent transaction", function() {
      this.childTx.rollback();
      this.tx.rollback();
    })

    it("deletes nodes written by parent transaction when commiting child transaction", function() {
      let node = { keys: [1], values: [1] };
      this.tx.beginWrite(node);
      let ptr = node[PTR];
      this.tx.endWrite(node);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.delete(ptr);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.commit();
      expect(this.store.nodes[ptr]).to.be.undefined;
    })

    it("deletes nodes written by child transaction when rolling back parent transaction", function() {
      let node = { keys: [1], values: [1] };
      this.childTx.beginWrite(node);
      let ptr = node[PTR];
      this.childTx.endWrite(node);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.commit();
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.tx.rollback();
      expect(this.store.nodes[ptr]).to.be.undefined;
    })
  })
})
