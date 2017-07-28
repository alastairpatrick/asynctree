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
    this.ptr1 = "06/e2e90fd816bb9042ee601a9468654b";
    this.dir1 = "06";

    this.node2 = {
      keys: [2],
      values: [2],
    };
    this.ptr2 = "b2/48211fa97986189042d075e431779c";
    this.dir2 = "b2";


    sh.mkdir("-p", TEMP_DIR);
    sh.rm("-rf", join(TEMP_DIR, "*"));

    return FileStore.create(TEMP_DIR, {
      cacheSize: Infinity,
      compress: false,
    }).then(store => {
      return FileStore.create(TEMP_DIR, {
        cacheSize: Infinity,
        compress: false,
      }).then(store2 => {
        this.store = store;
        this.store2 = store2;

        this.store.sessionId = "00000001";
        this.store.sessionId = "00000002";
      });
    });
  })

  afterEach(function() {
    return this.store.flush().then(() => {
      sh.rm("-rf", join(TEMP_DIR, "*"));
    });
  });

  it("can read written nodes", function() {
    this.store.write(this.node1);
    expect(this.node1.id).to.equal("0000000200000000");
    return this.store.read(this.node1[PTR]).then(node => {
      expect(node.id).to.equal("0000000200000000");
      expect(node).to.deep.equal(this.node1);
      expect(node).to.equal(this.node1);
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
      children$: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    expect(parentNode[PTR].toJSON()).to.equal("d4/62b9c4ceb9deab734bee8f0adc9e27");
  })

  it("writes nodes to files on flush", function() {
    this.store.write(this.node1);
    let parentNode = {
      keys: [],
      children$: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    return this.store.flush().then(() => {
      expect(existsSync(join(TEMP_DIR, "node", "d4/62b9c4ceb9deab734bee8f0adc9e27"))).to.be.true;
      expect(existsSync(join(TEMP_DIR, "node", this.ptr1))).to.be.true;
    });
  })

  it("does not write deleted nodes to files on flush", function() {
    this.store.write(this.node1);
    let parentNode = {
      keys: [],
      children$: [this.node1[PTR]],
    };
    this.store.write(parentNode);
    this.store.delete(parentNode[PTR]);
    return this.store.flush().then(() => {
      expect(existsSync(join(TEMP_DIR, "node", this.ptr1))).to.be.true;
    });
  })

  it("nodes evicted from write cache are written to file", function() {
    this.store.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);  // evicts node1
    let path = join(TEMP_DIR, "node", this.ptr1);
    return this.store.pathTasks.get(path).promise.then(() => {
      expect(readdirSync(join(TEMP_DIR, "node"))).to.deep.equal([this.dir1]);
      expect(existsSync(path)).to.be.true;
    });
  })

  it("nodes evicted from write cache and written to file can be deleted", function() {
    this.store.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);  // evicts node1
    let path = join(TEMP_DIR, "node", this.ptr1);
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
    let path = join(TEMP_DIR, "node", this.ptr1);
    return this.store.pathTasks.get(path).promise.then(() => {
      expect(existsSync(path)).to.be.true;
      
      return this.store.read(this.node1[PTR]).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    });
  })

  it("can read nodes written by other store", function() {
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      return this.store2.read(this.ptr1).then(node => {
        expect(node[PTR]).to.equal(this.ptr1);
        expect(node).to.deep.equal(this.node1);
        expect(node).to.not.equal(this.node1);
      });
    });
  })

  it("cannot delete nodes written by other store", function() {
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      return this.store2.delete(this.ptr1).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/delete/);
      });
    });
  })

  it("caches nodes read from file", function() {
    this.store.write(this.node1);
    return this.store.flush().then(() => {
      return this.store2.read(this.ptr1).then(node => {
        expect(this.store2.cache.get(this.ptr1)).to.deep.equal(node);
        return this.store2.read(this.ptr1).then(node2 => {
          expect(node2).to.deep.equal(node);
        });
      });
    });
  })

  it("evicts nodes from read cache", function() {
    this.store2.config.cacheSize = 1;
    this.store.write(this.node1);
    this.store.write(this.node2);
    return this.store.flush().then(() => {
      return this.store2.read(this.ptr1).then(node1 => {
        return this.store2.read(this.ptr2).then(node2 => {  // evicts node1
          expect(this.store2.cache.get(this.ptr2)).to.deep.equal(node2);
          expect(this.store2.cache.has(this.ptr1)).to.be.false;
        });
      });
    });
  })

  it("can read written compressed nodes", function() {
    this.store2.config.compress = true;
    this.store2.write(this.node1);
    return this.store2.read(this.node1[PTR]).then(node => {
      expect(node).to.deep.equal(this.node1);
      expect(node).to.equal(this.node1);
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
    this.store.compress = false;
    this.store.config.fileMode = 0o666;  // So node files can be corrupted easily
    this.store.write(this.node1);
    return this.store.sync().then(() => {
      // Corrupt the node file
      writeFileSync(join(TEMP_DIR, "node", String(this.ptr1)), "!");

      return this.store.read(this.ptr1).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/hash/);
      });
    });
  })
  
  it("links node from other store", function() {
    return FileStore.create(join(TEMP_DIR, "store2"), {
      cacheSize: Infinity,
      compress: false,
    }).then(store2 => {
      this.store.write(this.node1);
      return store2.copy(this.store, this.ptr1).then(() => {
        return store2.copy(this.store, this.ptr1); // second time to test overwriting
      }).then(() => {
        return store2.read(this.ptr1);
      }).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    });
  })
  
  it("copies node from other store", function() {
    return FileStore.create(join(TEMP_DIR, "store2"), {
      cacheSize: Infinity,
      compress: false,
    }).then(store2 => {
      this.store.write(this.node1);
      return store2.copy(this.store, this.ptr1, { tryLink: false }).then(() => {
        return store2.copy(this.store, this.ptr1, { tryLink: false }); // second time to test overwriting
      }).then(() => {
        return store2.read(this.ptr1);
      }).then(node => {
        expect(node).to.deep.equal(this.node1);
      });
    });
  })
  
  it("touches node", function() {
    this.store.write(this.node1);
    return this.store.copy(this.store, this.ptr1, { touch: true }).then(() => {
      return this.store.read(this.ptr1);
    }).then(node => {
      expect(node).to.deep.equal(this.node1);
    });
  })

  it("returns meta path", function() {
    expect(this.store.metaPath()).to.equal(join(TEMP_DIR, "meta"));
  });

  it("writes meta file", function() {
    return this.store.writeMeta({ rootPtr$: this.ptr1 }).then(meta => {
      expect(meta).to.deep.equal({ rootPtr$: this.ptr1 });
      let metaPath = join(TEMP_DIR, "meta");
      expect(JSON.parse(readFileSync(metaPath, { encoding: "utf-8" }))).to.deep.equal({ rootPtr$: this.ptr1 });
    });
  });

  it("transforms meta file", function() {
    let promises = [];
    for (let i = 0; i < 3; ++i) {
      promises.push(this.store.writeMeta(meta => {
        meta[i] = i;
        return meta;
      }));
    }
    return Promise.all(promises).then(() => {
      let metaPath = join(TEMP_DIR, "meta");
      expect(JSON.parse(readFileSync(metaPath, { encoding: "utf-8" }))).to.deep.equal({
        "0": 0,
        "1": 1,
        "2": 2,
      });
    });
  });

  it("reads meta file", function() {
    return this.store.writeMeta({ rootPtr$: this.ptr1 }).then(() => {
      return this.store.readMeta();
    }).then(meta => {
      expect(meta).to.deep.equal({ rootPtr$: this.ptr1 });
    });
  });

  it("exception if file mode does not allow read and write access to user", function() {
    return FileStore.create(TEMP_DIR, { fileMode: 0o444 }).then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/access/);
    });
  });
})
