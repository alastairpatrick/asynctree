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
  })

  afterEach(function() {
    return this.store.flush().then(() => {
      sh.rm("-rf", join(TEMP_DIR, "*"));
    });
  });

  it("assigns pointers to written nodes", function() {
    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    this.store.write(this.node2);
    let ptr2 = this.node2[PTR];
    expect(ptr1).to.equal(this.ptr1);
    expect(ptr2).to.equal(this.ptr2);
  })

  it("can read cached written nodes before flush", function() {
    this.store.write(this.node1);
    let ptr = this.node1[PTR];
    return this.store.read(ptr).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr);
      expect(node).to.not.equal(this.node1);
    });
  })

  it("writes nodes to files on flush", function() {
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      let nodePath = join(TEMP_DIR, this.ptr1);
      expect(JSON.parse(readFileSync(nodePath))).to.deep.equal(this.node1);
    });
  })

  it("does not write nodes to file while initially cached", function() {
    this.store.write(this.node1);
    expect(readdirSync(join(TEMP_DIR))).to.deep.equal([]);
  })

  it("reads written node from file after flush", function() {
    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    return this.store.flush().then(() => {
      return this.store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr1);
    });
  })

  it("does not rewrite nodes to files in subsequent flushes", function() {
    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    return this.store.flush().then(() => {
      expect(readdirSync(join(TEMP_DIR, this.dir1))).to.deep.equal([this.ptr1.substring(3)]);
      return this.store.flush();
    }).then(node => {
      expect(readdirSync(join(TEMP_DIR, this.dir1))).to.deep.equal([this.ptr1.substring(3)]);
    });
  })

  it("cannot read nodes after deleting", function() {
    this.store.write(this.node1);
    let ptr = this.node1[PTR];
    this.store.delete(ptr);
    return this.store.read(ptr).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error.code).to.equal("ENOENT");
    });
  })

  it("reads compressed node from file after flush", function() {
    this.store.config.compress = true;
    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    return this.store.flush().then(() => {
      return this.store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr1);
    });
  })

  it("reads and verifies compressed node from file after flush", function() {
    this.store.config.verifyHash = true;
    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    return this.store.flush().then(() => {
      return this.store.read(ptr1);
    }).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node[PTR]).to.equal(ptr1);
    });
  })

  it("throws exception of failed verification", function() {
    this.store.config.verifyHash = true;
    this.store.config.fileMode = 0o666;  // So node files can be corrupted easily

    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];
    return this.store.sync().then(() => {
      let path = join(TEMP_DIR, ptr1);

      // Corrupt the node file
      writeFileSync(path, "!");

      return this.store.read(ptr1);
    }).then(node => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/hash digest/);
    });
  })

  it("nodes evicted from cache are written to file", function() {
    this.store.config.cacheSize = 1;

    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];

    this.store.write(this.node2);
    let ptr2 = this.node2[PTR];

    expect(this.store.discrepancies.size).to.equal(1);
    return this.store.flush().then(() => {
      expect(this.store.discrepancies.size).to.equal(0);
      expect(readdirSync(join(TEMP_DIR, this.dir1))).to.deep.equal([this.ptr1.substring(3)]);
    })
  })

  it("nodes evicted from cache can be read again after they finish writing", function() {
    this.store.config.cacheSize = 1;

    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];

    this.store.write(this.node2);
    let ptr2 = this.node2[PTR];

    expect(this.store.discrepancies.size).to.equal(1);
    return this.store.flush().then(() => {
      expect(this.store.discrepancies.size).to.equal(0);
      return this.store.read(ptr1).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    })
  })

  it("nodes evicted from cache can be read while they are still being written", function() {
    this.store.config.cacheSize = 1;

    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];

    this.store.write(this.node2);
    let ptr2 = this.node2[PTR];

    expect(this.store.discrepancies.size).to.equal(1);
    return this.store.read(ptr1).then(node => {
      expect(node).to.deep.equal(this.node1);
    });
  })

  it("nodes evicted from cache can be deleted while they are still being written", function() {
    this.store.config.cacheSize = 1;

    this.store.write(this.node1);
    let ptr1 = this.node1[PTR];

    this.store.write(this.node2);
    let ptr2 = this.node2[PTR];

    this.store.delete(ptr1);

    expect(this.store.discrepancies.size).to.equal(1);
    return this.store.flush().then(() => {
      expect(this.store.discrepancies.size).to.equal(0);
      expect(readdirSync(join(TEMP_DIR, this.dir1))).to.deep.equal([]);
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
