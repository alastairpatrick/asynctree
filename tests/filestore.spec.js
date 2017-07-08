"use strict";

const { expect } = require("chai");
const { existsSync, readdirSync, readFileSync, statSync, writeFileSync } = require("fs");
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
    this.ptr1 = "8a/e9b8c4137941181c067514bdcb2371";
    this.dir1 = "8a";

    this.node2 = {
      keys: [2],
      values: [2],
    };
    this.ptr2 = "de/6cdc8f57b0b7af87574f6ff128a297";
    this.dir2 = "de";

    sh.rm("-rf", join(TEMP_DIR, "*"));

    this.store = new FileStore(TEMP_DIR, {
      cacheSize: Infinity,
      compress: false,
    });

    this.store2 = new FileStore(TEMP_DIR, {
      cacheSize: Infinity,
      compress: false,
    });
  })

  afterEach(function() {
    return this.store.flush().then(() => {
      sh.rm("-rf", join(TEMP_DIR, "*"));
    });
  });

  it("can read written nodes", function() {
    this.store.write(this.node1);
    return this.store.read(this.node1[PTR]).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node).to.not.equal(this.node1);
    });
  })

  it("cannot read deleted nodes", function() {
    this.store.write(this.node1);
    this.store.delete(this.node1[PTR]);
    return this.store.read(this.node1[PTR]).then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/delete/);
    });
  })

  it("JSON representation of pointer is hash of Merkle tree rooted at corresponding node", function() {
    this.store.write(this.node1);
    let parentNode = {
      keys: [],
      children: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    expect(parentNode[PTR].toJSON()).to.equal("f0/acd7edc9c4c24a1351b80e68e6d830");
  })

  it("writes nodes to files on flush", function() {
    this.store.write(this.node1);
    let parentNode = {
      keys: [],
      children: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    return this.store.flush().then(() => {
      expect(existsSync(join(TEMP_DIR, "f0/acd7edc9c4c24a1351b80e68e6d830"))).to.be.true;
      expect(existsSync(join(TEMP_DIR, this.ptr1))).to.be.true;
    });
  })

  it("does not write deleted nodes to files on flush", function() {
    this.store.write(this.node1);
    let parentNode = {
      keys: [],
      children: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    this.store.delete(parentNode[PTR]);
    return this.store.flush().then(() => {
      expect(existsSync(join(TEMP_DIR, this.ptr1))).to.be.true;
    });
  })

  it("nodes evicted from write cache are written to file", function() {
    this.store.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);  // evicts node1
    let path = join(TEMP_DIR, this.ptr1);
    return this.store.pathTasks.get(path).promise.then(() => {
      expect(readdirSync(TEMP_DIR)).to.deep.equal([this.dir1]);
      expect(existsSync(path)).to.be.true;
    });
  })

  it("nodes evicted from write cache and written to file can be deleted", function() {
    this.store.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);  // evicts node1
    let path = join(TEMP_DIR, this.ptr1);
    return this.store.pathTasks.get(path).promise.then(() => {
      expect(existsSync(path)).to.be.true;
      
      this.store.delete(this.node1[PTR]);
      return this.store.pathTasks.get(path).promise.then(() => {
        expect(existsSync(path)).to.be.false;
      });
    });
  })

  it("nodes evicted from write cache and written to file can be read", function() {
    this.store.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);  // evicts node1
    let path = join(TEMP_DIR, this.ptr1);
    return this.store.pathTasks.get(path).promise.then(() => {
      expect(existsSync(path)).to.be.true;
      
      return this.store.read(this.node1[PTR]).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    });
  })

  it("can read nodes written by other store", function() {
    this.store2.write(this.node1);
    return this.store2.flush().then(() => {
      return this.store.read(this.ptr1).then(node => {
        expect(node[PTR]).to.equal(this.ptr1);
        expect(node).to.deep.equal(this.node1);
        expect(node).to.not.equal(this.node1);
      });
    });
  })

  it("cannot delete nodes written by other store", function() {
    this.store2.write(this.node1);
    return this.store2.flush().then(() => {
      return this.store.delete(this.ptr1).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/delete/);
      });
    });
  })

  it("caches nodes read from file", function() {
    this.store2.write(this.node1);
    return this.store2.flush().then(() => {
      return this.store.read(this.ptr1).then(node => {
        expect(this.store.cache.get(this.ptr1)).to.deep.equal(node);
        return this.store.read(this.ptr1).then(node2 => {
          expect(node2).to.deep.equal(node);
        });
      });
    });
  })

  it("evicts nodes from read cache", function() {
    this.store.config.cacheSize = 1;
    this.store2.write(this.node1);
    this.store2.write(this.node2);
    return this.store2.flush().then(() => {
      return this.store.read(this.ptr1).then(node1 => {
        return this.store.read(this.ptr2).then(node2 => {  // evicts node1
          expect(this.store.cache.get(this.ptr2)).to.deep.equal(node2);
          expect(this.store.cache.has(this.ptr1)).to.be.false;
        });
      });
    });
  })

  it("can read written compressed nodes", function() {
    this.store.config.compress = true;
    this.store.write(this.node1);
    return this.store.read(this.node1[PTR]).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node).to.not.equal(this.node1);
    });
  })

  it("verifies hash on read from file", function() {
    this.store.config.verifyHash = true;
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      this.store.read(this.ptr1).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    });
  })

  it("throws exception if hash doesn't match file", function() {
    this.store.config.verifyHash = true;
    this.store.config.fileMode = 0o666;  // So node files can be corrupted easily
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      // Corrupt the node file
      writeFileSync(join(TEMP_DIR, String(this.ptr1)), "!");

      return this.store.read(this.ptr1).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/hash/);
      });
    });
  })

  it("writes index", function() {
    return this.store.writeIndexPtr(this.ptr1).then(() => {
      let indexPath = join(TEMP_DIR, "index");
      expect(readFileSync(indexPath, { encoding: "utf-8" })).to.equal(this.ptr1);
    });
  });

  it("reads index", function() {
    return this.store.writeIndexPtr(this.ptr1).then(() => {
      return this.store.readIndexPtr();
    }).then(ptr => {
      expect(ptr).to.equal(this.ptr1);      
    });
  });
})
