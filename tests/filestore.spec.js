"use strict";

const { expect } = require("chai");
const { readdirSync, readFileSync, statSync } = require("fs");
const { dirname, join } = require("path");
const sh = require("shelljs");
const sinon = require("sinon");

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
      store.cacheSize = Infinity;
    });
  })

  afterEach(function() {
    return store.flush().then(() => {
      sh.rm("-rf", join(TEMP_DIR, "*"));
    });
  });

  it("creates session directrory", function() {
    let sessionDir = join(TEMP_DIR, store.sessionName);
    expect(statSync(sessionDir).isDirectory()).to.be.true;
    expect(readdirSync(sessionDir)).to.deep.equal([]);
  })

  it("creates index directrory", function() {
    let indexDir = join(TEMP_DIR, "index");
    expect(statSync(indexDir).isDirectory()).to.be.true;
  })

  it("assigns pointers to written nodes", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    store.beginWrite(node2);
    let ptr2 = node2[PTR];
    store.endWrite(node2);
    expect(ptr1).to.equal(store.sessionName + "/000000");
    expect(ptr2).to.equal(store.sessionName + "/000001");
  })

  it("can read cached written nodes before flush", function() {
    store.beginWrite(node1);
    let ptr = node1[PTR];
    store.endWrite(node1);
    return store.read(ptr).then(node => {
      expect(node).to.deep.equal(node1);
      expect(node[PTR]).to.equal(ptr);
      expect(node).to.not.equal(node1);
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

  it("writes nodes to files on flush", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    return store.flush().then(() => {
      let nodePath = join(TEMP_DIR, store.sessionName, "000000");
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(node1);
    });
  })

  it("does not write nodes to file while initially cached", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    expect(readdirSync(join(TEMP_DIR, store.sessionName))).to.deep.equal([]);
  })

  it("reads written node from file after flush", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    return store.flush().then(() => {
      return store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(node1);
      expect(node[PTR]).to.equal(ptr1);
    });
  })

  it("does not rewrite nodes to files in subsequent flushes", function() {
    store.beginWrite(node1);
    let ptr1 = node1[PTR];
    store.endWrite(node1);
    return store.flush().then(() => {
      expect(readdirSync(join(TEMP_DIR, store.sessionName))).to.deep.equal(["000000"]);
      return store.flush();
    }).then(node => {
      expect(readdirSync(join(TEMP_DIR, store.sessionName))).to.deep.equal(["000000"]);
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
      expect(readdirSync(join(TEMP_DIR, store.sessionName))).to.deep.equal(["000000"]);
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
      expect(readdirSync(join(TEMP_DIR, store.sessionName))).to.deep.equal([]);
    });
  })

  it("commits root node", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    return store.commit(node1[PTR], "myroot").then(() => {
      let nodePath = join(TEMP_DIR, "index", "myroot");
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(node1);
    });
  })

  it("commits root node over existing", function() {
    store.beginWrite(node1);
    store.endWrite(node1);
    return store.commit(node1[PTR], "myroot").then(() => {
      return store.commit(node1[PTR], "myroot").then(() => {
        let nodePath = join(TEMP_DIR, "index", "myroot");
        expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(node1);
      });
    });
  })
})
