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

const TEMP_DIR = join(__dirname, "temp");

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

    return FileStore.newSession(TEMP_DIR).then(store_ => {
      store = store_;
      store.cacheSize = 1000000;
    });
  })

  afterEach(function() {
    return store.rollback().then(() => {
      sh.rm("-rf", join(TEMP_DIR, store.sessionName));
    });
  });

  it("creates session and transaction directrory", function() {
    let sessionDir = join(TEMP_DIR, store.sessionName);
    let transactionDir = join(sessionDir, "000000");
    expect(statSync(sessionDir).isDirectory()).to.be.true;
    expect(readdirSync(sessionDir)).to.deep.equal(["000000"]);
    expect(statSync(transactionDir).isDirectory()).to.be.true;
    expect(readdirSync(transactionDir)).to.deep.equal([]);
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

  it("writes nodes to files on commit", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    return store.commit(tree).then(() => {
      let commitDir = join(TEMP_DIR, store.sessionName, "000000");
      let nodePath = join(commitDir, "000000");
      expect(statSync(commitDir).isDirectory()).to.be.true;
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(node1);
    });
  })

  it("does not write nodes to file while initially cached", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    let commitDir = join(TEMP_DIR, store.sessionName, "000000");
    expect(readdirSync(commitDir)).to.deep.equal([]);
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
      let commitDir = join(TEMP_DIR, store.sessionName, "000001");
      expect(statSync(commitDir).isDirectory()).to.be.true;
      expect(readdirSync(commitDir)).to.deep.equal([]);
    });
  })

  it("cannot read nodes after deleting", function() {
    store.beginWrite(node1);
    let ptr = node1[PTR];
    store.endWrite(node1);
    store.delete(ptr);
    return store.read(ptr).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });
  })

  it("cannot delete node file written in prior transaction", function() {
    store.beginWrite(node1);
    let ptr = node1[PTR];
    store.endWrite(node1);
    let commitDir = join(TEMP_DIR, store.sessionName, "000000");
    return store.commit().then(() => {
      let commitDir = join(TEMP_DIR, store.sessionName, "000000");
      expect(readdirSync(commitDir)).to.deep.equal(["000000"]);
      store.delete(ptr);
      expect(readdirSync(commitDir)).to.deep.equal(["000000"]);
    });
  })
  
  it("rollback deletes nodes written in current transaction", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    return store.rollback().then(() => {
      store.read(ptr1).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error.code).to.equal("ENOENT");
      });

      return store.commit(tree).then(() => {
        let commitDir = join(TEMP_DIR, store.sessionName, "000000");
        expect(statSync(commitDir).isDirectory()).to.be.true;
        expect(readdirSync(commitDir)).to.deep.equal([]);
      });
    });
  })

  it("rollback does not delete nodes written in prior transaction", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);

    return store.commit().then(() => {
      store.beginWrite(node2);
      let ptr2 = node2[PTR];
      store.endWrite(node2);
    
      return store.rollback().then(() => {
        return store.read(ptr2).then(node => {
          expect.fail("Did not throw");
        }).catch(error => {
          expect(error.code).to.equal("ENOENT");
        });
      }).then(() => {
        return store.read(ptr1).then(node => {
          expect(node).to.deep.equal(node1);
        });
      });
    });
  })

  it("nodes evicted from cache are written to file", function() {
    store.cacheSize = 1;

    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);

    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);

    expect(store.writing.size).to.equal(1);
    return store.writing.get(ptr1).promise.then(() => {
      let commitDir = join(TEMP_DIR, store.sessionName, "000000");
      expect(readdirSync(commitDir)).to.deep.equal(["000000"]);
    })
  })

  it("nodes evicted from cache can be read again after they finish writing", function() {
    store.cacheSize = 1;

    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);

    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);

    expect(store.writing.size).to.equal(1);
    return store.writing.get(ptr1).promise.then(() => {
      return store.read(ptr1).then(node => {
        expect(node).to.deep.equal(node1);
      });
    })
  })

  it("nodes evicted from cache can be read while they are still being written", function() {
    store.cacheSize = 1;

    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);

    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);

    expect(store.writing.size).to.equal(1);
    return store.read(ptr1).then(node => {
      expect(node).to.deep.equal(node1);
    });
  })

  it("nodes evicted from cache can be deleted while they are still being written", function() {
    store.cacheSize = 1;

    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);

    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);

    store.delete(ptr1);

    expect(store.writing.size).to.equal(1);
    return store.writing.get(ptr1).promise.then(() => {
      let commitDir = join(TEMP_DIR, store.sessionName, "000000");
      expect(readdirSync(commitDir)).to.deep.equal([]);
    });
  })
})
