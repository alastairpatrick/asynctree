"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { PTR, Rollback, cloneNode } = require("..");

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

describe("Rollback", function() {
  beforeEach(function() {
    this.store = new TestStore();
    this.transaction = new Rollback(this.store);
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

  it("only rolls back changes since last commit", function() {
    let node1 = { keys: [1], values: [1] };
    this.transaction.beginWrite(node1);
    let ptr1 = node1[PTR];
    this.transaction.endWrite(node1);
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);

    this.transaction.commit();

    let node2 = { keys: [2], values: [2] };
    this.transaction.beginWrite(node2);
    let ptr2 = node2[PTR];
    this.transaction.endWrite(node2);
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.deep.equal(node2);

    this.transaction.rollback();
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.be.undefined;
  })

  it("may not rollback twice", function() {
    let node = { keys: [1], values: [1] };
    this.transaction.beginWrite(node);
    let ptr = node[PTR];
    this.transaction.endWrite(node);
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    this.transaction.rollback();
    this.transaction.rollback();
    expect(this.store.nodes[ptr]).to.be.undefined;
  })
})
