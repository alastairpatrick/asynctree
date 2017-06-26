"use strict";

const { expect } = require("chai");
const { readdirSync, readFileSync, statSync } = require("fs");
const { dirname, join } = require("path");
const sh = require("shelljs");
const sinon = require("sinon");
const cloneDeep = require("lodash/cloneDeep");

const { PTR } = require("..");
const { FileStore } = require("../filestore");

const has = Object.prototype.hasOwnProperty;

const TEST_DIR = join(__dirname, "temp");

describe("FileStore", function() {
  let store;
  let node1, node2;
  let tree;

  beforeEach(function() {
    node1 = {
      keys: [1],
      values: [1],
    };

    node2 = {
      keys: [1],
      values: [1],
    };

    tree = {
      mark(cb) {
        for (let ptr in store.nodes) {
          if (has.call(store.nodes, ptr)) {
            cb(ptr);
          }
        }
        return Promise.resolve();
      }
    };

    return FileStore.newSession(TEST_DIR).then(store_ => {
      store = store_;
    });
  })

  afterEach(function() {
    sh.rm("-rf", join(TEST_DIR, store.sessionName));
  });

  it("creates empty session directrory", function() {
    let sessionDir = join(TEST_DIR, store.sessionName);
    expect(statSync(sessionDir).isDirectory()).to.be.true;
    expect(readdirSync(sessionDir)).to.deep.equal([]);
  })

  it("assigns pointers to written nodes", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);
    expect(ptr1).to.equal(store.sessionName + "/000000/000000");
    expect(ptr2).to.equal(store.sessionName + "/000000/000001");
  })

  it("can read written nodes before commit", function() {
    store.beginWrite(node1);
    let ptr = node1[PTR];
    store.endWrite(node1);
    return store.read(ptr).then(node => {
      expect(node).to.equal(node1);
    });
  })

  it("cannot read written nodes before endWrite", function() {
    store.beginWrite(node1);
    let ptr = node1[PTR];
    return store.read(ptr).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });
  })

  it("writes written nodes to files on commit", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    return store.commit(tree).then(() => {
      let commitDir = join(TEST_DIR, store.sessionName, "000000");
      let nodePath = join(commitDir, "000000");
      expect(statSync(commitDir).isDirectory()).to.be.true;
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(node1);
    });
  })

  it("reads written node from file after commit", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    return store.commit(tree).then(() => {
      return store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(node1);
    });
  })

  it("does not rewrite nodes to files in subsequent commits", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    return store.commit(tree).then(() => {
      return store.commit(tree);
    }).then(node => {
      let commitDir = join(TEST_DIR, store.sessionName, "000001");
      expect(statSync(commitDir).isDirectory()).to.be.true;
      expect(readdirSync(commitDir)).to.deep.equal([]);
    });
  })

  it("rollback cancels writes", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    store.rollback();
    store.read(ptr1).then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });

    return store.commit(tree).then(() => {
      let commitDir = join(TEST_DIR, store.sessionName, "000000");
      expect(statSync(commitDir).isDirectory()).to.be.true;
      expect(readdirSync(commitDir)).to.deep.equal([]);
    });
  })
})
