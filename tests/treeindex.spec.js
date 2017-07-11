"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sinon = require("sinon");

const { TreeIndex, PTR, cloneNode } = require("..");
const { TestStore } = require("./teststore");

const has = Object.prototype.hasOwnProperty;


describe("TreeIndex", function() {
  beforeEach(function() {
    this.store = new TestStore();
    return TreeIndex.open(this.store, "index", {
      index: "config",
    }).then(index_ => {
      this.treeIndex = index_;
    });
  })

  it("creates empty tree", function() {
    let tree = this.treeIndex.empty({
      tree: "config",
    });
    expect(tree.config).to.deep.equal({
      index: "config",
      tree: "config",
      order: 1024,
    });
    return this.store.read(tree.rootPtr).then(root => {
      expect(root).to.deep.equal({
        keys: [],
        values: [],
      });
    });
  })

  it("opens existing tree", function() {
    let tree = this.treeIndex.empty({
      tree: "config",
    });
    return tree.insert(1, 10).then(() => {
      return this.treeIndex.commit({ mytree: tree });
    }).then(() => {
      return this.treeIndex.open("mytree").then(tree2 => {
        expect(tree2.config).to.deep.equal({
          index: "config",
          tree: "config",
          order: 1024,
        });
        return tree2.get(1);
      }).then(value => {
        expect(value).to.equal(10);
      });
    });
  })

  it("commit flushes store", function() {
    sinon.spy(this.store, "flush");
    let tree = this.treeIndex.empty({
      tree: "config",
    });
    return tree.insert(1, 10).then(() => {
      return this.treeIndex.commit({ mytree: tree });
    }).then(() => {
      sinon.assert.calledOnce(this.store.flush);
    });
  })

  it("exception on opening tree that does not exist", function() {
    return this.treeIndex.open("mytree").then(tree2 => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/'mytree'/);
    });
  })

  it("deletes existing tree", function() {
    let tree = this.treeIndex.empty();
    return tree.insert(1, 10).then(() => {
      return this.treeIndex.commit({ mytree: tree });
    }).then(() => {
      return this.treeIndex.commit({ mytree: undefined });
    }).then(() => {
      return this.treeIndex.open("mytree");
    }).then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/'mytree'/);
    });
  })

  it("commits over existing tree", function() {
    let tree = this.treeIndex.empty();
    return tree.insert(1, 10).then(() => {
      return this.treeIndex.commit({ mytree: tree });
    }).then(() => {
      return tree.update(1, 100);
    }).then(() => {
      return this.treeIndex.commit({ mytree: tree });
    }).then(() => {
      return this.treeIndex.open("mytree").then(tree2 => {
        return tree2.get(1);
      }).then(value => {
        expect(value).to.equal(100);
      });
    });
  })
})
