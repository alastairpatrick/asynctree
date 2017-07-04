"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sinon = require("sinon");

const { TreeIndex, PTR, cloneNode } = require("..");

const has = Object.prototype.hasOwnProperty;

class TestStore {
  constructor() {
    this.trees = {};
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

  readTreeIndex() {
    return Promise.resolve(this.trees);
  }

  writeTreeIndex(trees) {
    this.trees = trees;
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

describe("TreeIndex", function() {
  beforeEach(function() {
    this.store = new TestStore();
    return TreeIndex.open(this.store, {
      forest: "config",
    }).then(forest_ => {
      this.forest = forest_;
    });
  })

  it("creates empty tree", function() {
    let tree = this.forest.empty({
      tree: "config",
    });
    expect(tree.config).to.deep.equal({
      forest: "config",
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

  it("openes existing tree", function() {
    let tree = this.forest.empty({
      tree: "config",
    });
    return tree.insert(1, 10).then(() => {
      return this.forest.commit({ mytree: tree });
    }).then(() => {
      let tree2 = this.forest.open("mytree").then(tree2 => {
        expect(tree2.config).to.deep.equal({
          forest: "config",
          tree: "config",
          order: 1024,
        });
        return tree2.get(1);
      }).then(value => {
        expect(value).to.equal(10);
      });
    });
  })

  it("exception on opening tree that does not exist", function() {
    return this.forest.open("mytree").then(tree2 => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/'mytree'/);
    });
  })

  it("deletes existing tree", function() {
    let tree = this.forest.empty();
    return tree.insert(1, 10).then(() => {
      return this.forest.commit({ mytree: tree });
    }).then(() => {
      return this.forest.commit({ mytree: undefined });
    }).then(() => {
      return this.forest.open("mytree");
    }).then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/'mytree'/);
    });
  })

  it("commits over existing tree", function() {
    let tree = this.forest.empty();
    return tree.insert(1, 10).then(() => {
      return this.forest.commit({ mytree: tree });
    }).then(() => {
      return tree.update(1, 100);
    }).then(() => {
      return this.forest.commit({ mytree: tree });
    }).then(() => {
      return this.forest.open("mytree").then(tree2 => {
        return tree2.get(1);
      }).then(value => {
        expect(value).to.equal(100);
      });
    });
  })
})
