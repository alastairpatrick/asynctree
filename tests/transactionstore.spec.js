"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { PTR, TransactionStore, cloneNode } = require("..");
const { TestStore } = require("./teststore");

const has = Object.prototype.hasOwnProperty;


describe("TransactionStore", function() {
  beforeEach(function() {
    this.store = new TestStore();
    this.tx = new TransactionStore(this.store, 77);
  })

  it("reads store", function() {
    this.store.nodes[1] = { keys: [1], values: [1] };
    this.tx.read(1).then(value => {
      expect(value).to.deep.equal(this.store.nodes[1]);
    });
  })

  it("rolls back newly written node", function() {
    let node = { keys: [1], values: [1] };
    this.tx.write(node);
    let ptr = node[PTR];
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    expect(this.tx.rollback()).to.equal(77);
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  it("deletes node immediately if it was written in current transaction", function() {
    let node = { keys: [1], values: [1] };
    this.tx.write(node);
    let ptr = node[PTR];
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
    this.tx.write(node1);
    let ptr1 = node1[PTR];
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);

    this.tx.commit(88);

    let node2 = { keys: [2], values: [2] };
    this.tx.write(node2);
    let ptr2 = node2[PTR];
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.deep.equal(node2);

    expect(this.tx.rollback()).to.equal(88);
    expect(this.store.nodes[ptr1]).to.deep.equal(node1);
    expect(this.store.nodes[ptr2]).to.be.undefined;
  })

  it("may not rollback twice", function() {
    let node = { keys: [1], values: [1] };
    this.tx.write(node);
    let ptr = node[PTR];
    expect(this.store.nodes[ptr]).to.deep.equal(node);
    this.tx.rollback();
    this.tx.rollback();
    expect(this.store.nodes[ptr]).to.be.undefined;
  })

  describe("nested", function() {
    beforeEach(function() {
      this.childTx = new TransactionStore(this.tx, 77);
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
      this.tx.write(node);
      let ptr = node[PTR];
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.delete(ptr);
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.commit();
      expect(this.store.nodes[ptr]).to.be.undefined;
    })

    it("deletes nodes written by child transaction when rolling back parent transaction", function() {
      let node = { keys: [1], values: [1] };
      this.childTx.write(node);
      let ptr = node[PTR];
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.childTx.commit();
      expect(this.store.nodes[ptr]).to.deep.equal(node);

      this.tx.rollback();
      expect(this.store.nodes[ptr]).to.be.undefined;
    })
  })
})
