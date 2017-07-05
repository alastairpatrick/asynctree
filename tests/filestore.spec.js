"use strict";

const { expect } = require("chai");
const { existsSync, readdirSync, readFileSync, statSync } = require("fs");
const { dirname, join } = require("path");
const sh = require("shelljs");
const sinon = require("sinon");

const { PTR } = require("..");
const { FileStore } = require("../filestore");

const has = Object.prototype.hasOwnProperty;

const TEMP_DIR = join(__dirname, "temp");

describe("FileStore", function() {
  beforeEach(function() {
    this.node1 = {
      keys: [1],
      values: [1],
    };

    this.node2 = {
      keys: [1],
      values: [1],
    };

    return FileStore.newSession(TEMP_DIR).then(store_ => {
      this.store = store_;
      this.store.cacheSize = Infinity;
    });
  })

  afterEach(function() {
    return this.store.flush().then(() => {
      sh.rm("-rf", join(TEMP_DIR, "*"));
    });
  });

  it("creates session directrory", function() {
    let sessionDir = join(TEMP_DIR, this.store.sessionName);
    expect(statSync(sessionDir).isDirectory()).to.be.true;
    expect(readdirSync(sessionDir)).to.deep.equal([]);
  })

  it("assigns pointers to written nodes", function() {
    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);
    this.store.beginWrite(this.node2);
    let ptr2 = this.node2[PTR];
    this.store.endWrite(this.node2);
    expect(ptr1).to.equal(this.store.sessionName + "/000000");
    expect(ptr2).to.equal(this.store.sessionName + "/000001");
  })

  it("can read cached written nodes before flush", function() {
    this.store.beginWrite(this.node1);
    let ptr = this.node1[PTR];
    this.store.endWrite(this.node1);
    return this.store.read(ptr).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr);
      expect(node).to.not.equal(this.node1);
    });
  })

  it("cannot read written nodes before endWrite", function() {
    this.store.beginWrite(this.node1);
    let ptr = this.node1[PTR];
    return this.store.read(ptr).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });
  })

  it("writes nodes to files on flush", function() {
    this.store.beginWrite(this.node1);
    this.store.endWrite(this.node1);
    return this.store.flush().then(() => {
      let nodePath = join(TEMP_DIR, this.store.sessionName, "000000");
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(this.node1);
    });
  })

  it("does not write nodes to file while initially cached", function() {
    this.store.beginWrite(this.node1);
    this.store.endWrite(this.node1);
    expect(readdirSync(join(TEMP_DIR, this.store.sessionName))).to.deep.equal([]);
  })

  it("reads written node from file after flush", function() {
    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);
    return this.store.flush().then(() => {
      return this.store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr1);
    });
  })

  it("does not rewrite nodes to files in subsequent flushes", function() {
    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);
    return this.store.flush().then(() => {
      expect(readdirSync(join(TEMP_DIR, this.store.sessionName))).to.deep.equal(["000000"]);
      return this.store.flush();
    }).then(node => {
      expect(readdirSync(join(TEMP_DIR, this.store.sessionName))).to.deep.equal(["000000"]);
    });
  })

  it("cannot read nodes after deleting", function() {
    this.store.beginWrite(this.node1);
    let ptr = this.node1[PTR];
    this.store.endWrite(this.node1);
    this.store.delete(ptr);
    return this.store.read(ptr).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });
  })

  it("nodes evicted from cache are written to file", function() {
    this.store.cacheSize = 1;

    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);

    this.store.beginWrite(this.node2);
    let ptr2 = this.node2[PTR];
    this.store.endWrite(this.node2);

    expect(this.store.writing.size).to.equal(1);
    return this.store.writing.get(ptr1).promise.then(() => {
      expect(readdirSync(join(TEMP_DIR, this.store.sessionName))).to.deep.equal(["000000"]);
    })
  })

  it("nodes evicted from cache can be read again after they finish writing", function() {
    this.store.cacheSize = 1;

    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);

    this.store.beginWrite(this.node2);
    let ptr2 = this.node2[PTR];
    this.store.endWrite(this.node2);

    expect(this.store.writing.size).to.equal(1);
    return this.store.writing.get(ptr1).promise.then(() => {
      return this.store.read(ptr1).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    })
  })

  it("nodes evicted from cache can be read while they are still being written", function() {
    this.store.cacheSize = 1;

    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);

    this.store.beginWrite(this.node2);
    let ptr2 = this.node2[PTR];
    this.store.endWrite(this.node2);

    expect(this.store.writing.size).to.equal(1);
    return this.store.read(ptr1).then(node => {
      expect(node).to.deep.equal(this.node1);
    });
  })

  it("nodes evicted from cache can be deleted while they are still being written", function() {
    this.store.cacheSize = 1;

    this.store.beginWrite(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.endWrite(this.node1);

    this.store.beginWrite(this.node2);
    let ptr2 = this.node2[PTR];
    this.store.endWrite(this.node2);

    this.store.delete(ptr1);

    expect(this.store.writing.size).to.equal(1);
    return this.store.writing.get(ptr1).promise.then(() => {
      expect(readdirSync(join(TEMP_DIR, this.store.sessionName))).to.deep.equal([]);
    });
  })

  it("writes index", function() {
    return this.store.writeIndexPtr("abcdef/000001").then(() => {
      let indexPath = join(TEMP_DIR, "index");
      expect(readFileSync(indexPath, { encoding: "utf-8" })).to.equal("abcdef/000001");
    });
  });

  it("reads index", function() {
    return this.store.writeIndexPtr("abcdef/000001").then(() => {
      return this.store.readIndexPtr();
    }).then(ptr => {
      expect(ptr).to.equal("abcdef/000001");      
    });
  });
})
