"use strict";

const { expect } = require("chai");
const sinon = require("sinon");
const cloneDeep = require("lodash/cloneDeep");

const { AsyncTree } = require("..");

const has = Object.prototype.hasOwnProperty;

class TestStore {
  constructor() {
    this.data = {};
    this.ptr = 1000;
  }

  read(ptr) {
    if (!has.call(this.data, ptr))
      return Promise.reject(new Error(`Pointer not found '${ptr}'.`));
    return Promise.resolve(this.data[ptr]);
  }

  write(node) {
    let ptr = ++this.ptr;
    this.data[ptr] = node;
    return ptr;
  }
}

const deserializeTree = (store, json) => {
  let node = Object.assign({}, json);
  
  if (!has.call(node, "children"))
    return Promise.resolve(store.write(node));

  let children = json.children.map(c => deserializeTree(store, c));
  return Promise.all(children).then(children => {
    node.children = children;
    return Promise.resolve(store.write(node));
  });
}

const serializeTree = (store, ptr) => {
  return store.read(ptr).then(tree => {
    let json = Object.assign({}, tree);

    if (!has.call(json, "children"))
      return json;

    let children = json.children.map(c => serializeTree(store, c));
    return Promise.all(children).then(children => {
      json.children = children;
      return json;
    });
  });
}

describe("AsyncTree", function() {
  let store;

  beforeEach(function() {
    store = new TestStore();
  })

  describe("round trips", function() {
    it("unit height tree", function() {
      return deserializeTree(store, {
        keys: [1, 2],
        values: [10, 20],
      }).then(ptr => {
        return serializeTree(store, ptr).then(tree => {
          expect(tree).to.deep.equal({
            keys: [1, 2],
            values: [10, 20],
          });
        });
      });
    })

    it("tree with internal nodes", function() {
      return deserializeTree(store, {
        keys: [2],
        children: [{
          keys: [1],
          values: [10],
        }, {
          keys: [2],
          values: [20],
        }],
      }).then(ptr => {
        return serializeTree(store, ptr).then(tree => {
          expect(tree).to.deep.equal({
            keys: [2],
            children: [{
              keys: [1],
              values: [10],
            }, {
              keys: [2],
              values: [20],
            }],
          });
        });
      });
    })
  })


  it("has no entries on creation", function() {
    let tree = new AsyncTree({ store });
    return serializeTree(store, tree.rootPtr).then(tree => {
      expect(tree).to.deep.equal({
        keys: [],
        values: [],
      });
    });
  })


  describe("set", function() {
    it("after creation", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(1, 10).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [1],
          values: [10],
        });
      });
    })

    it("entries are maintained in ascending in node", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(3, 30).then(tree => {
        return tree.insert(1, 10);
      }).then(tree => {
        return tree.insert(2, 20);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [1, 2, 3],
          values: [10, 20, 30],
        });
      });
    })

    it("can update value associated with key", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(1, 30).then(tree => {
        return tree.update(1, 10);
      }).then(tree => {
        return tree.insert(2, 20);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [1, 2],
          values: [10, 20],
        });
      });
    })

    it("exception on attempt to add key that already exists", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(1, 30).then(tree => {
        return tree.insert(1, 10);
      }).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/'1'/);
      });
    })

    it("exception on attempt to update key that does not exist", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(2, 20).then(tree => {
        return tree.update(1, 10);
      }).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/'1'/);
      });
    })

    // Tests identified with numbers here and elsewhere are based on examples taken from notes here:
    // http://www.cburch.com/cs/340/reading/btree/
    it("20", function() {
      return deserializeTree(store, {
        keys: [16],
        children: [{
          keys: [1, 4, 9],
          values: [1, 4, 9],
        }, {
          keys: [16, 25],
          values: [16, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(20, 20);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [16],
          children: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("13", function() {
      return deserializeTree(store, {
        keys: [16],
        children: [{
          keys: [1, 4, 9],
          values: [1, 4, 9],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(13, 13);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [9, 16],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 13],
            values: [9, 13],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("15", function() {
      return deserializeTree(store, {
        keys: [9, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 13],
          values: [9, 13],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(15, 15);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [9, 16],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 13, 15],
            values: [9, 13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("10", function() {
      return deserializeTree(store, {
        keys: [9, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 13, 15],
          values: [9, 13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(10, 10);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [9, 13, 16],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }, {
            keys: [13, 15],
            values: [13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("11", function() {
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10],
          values: [9, 10],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(11, 11);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [9, 13, 16],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10, 11],
            values: [9, 10, 11],
          }, {
            keys: [13, 15],
            values: [13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("12", function() {
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10, 11],
          values: [9, 10, 11],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.insert(12, 12);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [13],
          children: [{
            keys: [9, 11],
            children: [{
              keys: [1, 4],
              values: [1, 4],
            }, {
              keys: [9, 10],
              values: [9, 10],
            }, {
              keys: [11, 12],
              values: [11, 12],
            }],
          }, {
            keys: [16],
            children: [{
              keys: [13, 15],
              values: [13, 15],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        });
      });
    })
  })

  describe("delete", function() {
    it("sole value", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(1, 10).then(tree => {
        return tree.delete(1);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [],
          values: [],
        });
      });
    })
    
    it("throws exception if does not exist", function() {
      let tree = new AsyncTree({ store });
      return tree.insert(1, 10).then(tree => {
        return tree.delete(2);
      }).then(tree => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/'2'/);
      });
    })

    it("13", function() {
      return deserializeTree(store, {
        keys: [13],
        children: [{
          keys: [9, 11],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }, {
            keys: [11, 12],
            values: [11, 12],
          }],
        }, {
          keys: [16],
          children: [{
            keys: [13, 15],
            values: [13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.delete(13);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [13],
          children: [{
            keys: [9, 11],
            children: [{
              keys: [1, 4],
              values: [1, 4],
            }, {
              keys: [9, 10],
              values: [9, 10],
            }, {
              keys: [11, 12],
              values: [11, 12],
            }],
          }, {
            keys: [20],
            children: [{
              keys: [15, 16],
              values: [15, 16],
            }, {
              keys: [20, 25],
              values: [20, 25],
            }],
          }],
        });
      });
    })

    it("15", function() {
      return deserializeTree(store, {
        keys: [13],
        children: [{
          keys: [9, 11],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }, {
            keys: [11, 12],
            values: [11, 12],
          }],
        }, {
          keys: [20],
          children: [{
            keys: [15, 16],
            values: [15, 16],
          }, {
            keys: [20, 25],
            values: [20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.delete(15);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [11],
          children: [{
            keys: [9],
            children: [{
              keys: [1, 4],
              values: [1, 4],
            }, {
              keys: [9, 10],
              values: [9, 10],
            }],
          }, {
            keys: [13],
            children: [{
              keys: [11, 12],
              values: [11, 12],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        });
      });
    })


    it("1", function() {
      return deserializeTree(store, {
        keys: [11],
        children: [{
          keys: [9],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }],
        }, {
          keys: [13],
          children: [{
            keys: [11, 12],
            values: [11, 12],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.delete(1);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [11, 13],
          children: [{
            keys: [4, 9, 10],
            values: [4, 9, 10],
          }, {
            keys: [11, 12],
            values: [11, 12],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        });
      });
    })

    it("merges into rightmost leaf", function() {
      return deserializeTree(store, {
        keys: [9, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 13, 15],
          values: [9, 13, 15],
        }, {
          keys: [16, 20],
          values: [16, 20],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.delete(20);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [9, 15],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 13],
            values: [9, 13],
          }, {
            keys: [15, 16],
            values: [15, 16],
          }],
        });
      });
    })

    it("merges into leftmost internal node", function() {
      return deserializeTree(store, {
        keys: [11],
        children: [{
          keys: [9],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }],
        }, {
          keys: [15, 20],
          children: [{
            keys: [11, 12],
            values: [11, 12],
          }, {
            keys: [15, 16],
            values: [15, 16],
          }, {
            keys: [20, 25],
            values: [20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        tree.order = 2;
        return tree.delete(4);
      }).then(tree => {
        return serializeTree(store, tree.rootPtr);
      }).then(tree => {
        expect(tree).to.deep.equal({
          keys: [15],
          children: [{
            keys: [11],
            children: [{
              keys: [1, 9, 10],
              values: [1, 9, 10],
            }, {
              keys: [11, 12],
              values: [11, 12],
            }],
          }, {
            keys: [20],
            children: [{
              keys: [15, 16],
              values: [15, 16],
            }, {
              keys: [20, 25],
              values: [20, 25],
            }],
          }],
        });
      });
    })
  })


  describe("query", function() {
    it("gets particular record", function() {
      let tree = new AsyncTree({ store });
      let results = [];
      return tree.insert(2, 20).then(tree => {
        return tree.get(2);
      }).then(value => {
        expect(value).to.equal(20);
      });
    })

    it("gets returns undefined for missing record", function() {
      let tree = new AsyncTree({ store });
      let results = [];
      return tree.insert(2, 20).then(tree => {
        return tree.get(3);
      }).then(value => {
        expect(value).to.be.undefined;
      });
    })

    it("finds only record", function() {
      let tree = new AsyncTree({ store });
      let results = [];
      return tree.insert(2, 20).then(tree => {
        return tree.forEach((value, key) => {
          results.push([key, value]);
        });
      }).then(() => {
        expect(results).to.deep.equal([
          [2, 20],
        ]);
      });
    })

    it("finds two record", function() {
      let tree = new AsyncTree({ store });
      let results = [];
      return tree.insert(2, 20).then(tree => {
        return tree.insert(1, 10);
      }).then(tree => {
        return tree.forEach((value, key) => {
          results.push([key, value]);
        });
      }).then(() => {
        expect(results).to.deep.equal([
          [1, 10],
          [2, 20],
        ]);
      });
    })

    it("finds many record", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10, 11],
          values: [9, 10, 11],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.forEach((value, key) => {
          results.push([key, value]);
        });
      }).then(() => {
        expect(results).to.deep.equal([
          [1, 1],
          [4, 4],
          [9, 9],
          [10, 10],
          [11, 11],
          [13, 13],
          [15, 15],
          [16, 16],
          [20, 20],
          [25, 25],
        ]);
      });
    })

    it("finds many records until exception thrown by callback", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10, 11],
          values: [9, 10, 11],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.forEach((value, key) => {
          results.push([key, value]);
          if (key >= 13)
            throw new Error("fin");
        });
      }).then(() => {
        expect.fail("Did not throw");
      }).catch(error => {
        expect(error).to.match(/fin/);
        expect(results).to.deep.equal([
          [1, 1],
          [4, 4],
          [9, 9],
          [10, 10],
          [11, 11],
          [13, 13],
        ]);
      });
    })

    it("finds many records until callback rteturns STOP", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10, 11],
          values: [9, 10, 11],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.forEach((value, key) => {
          results.push([key, value]);
          if (key >= 13)
            return AsyncTree.BREAK;
        });
      }).then(() => {
        expect(results).to.deep.equal([
          [1, 1],
          [4, 4],
          [9, 9],
          [10, 10],
          [11, 11],
          [13, 13],
        ]);
      });
    })

    it("finds records in range [10, 16]", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [9, 13, 16],
        children: [{
          keys: [1, 4],
          values: [1, 4],
        }, {
          keys: [9, 10, 11],
          values: [9, 10, 11],
        }, {
          keys: [13, 15],
          values: [13, 15],
        }, {
          keys: [16, 20, 25],
          values: [16, 20, 25],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.rangeEach(10, 16, (value, key) => {
          results.push([key, value]);
        });
      }).then(() => {
        expect(results).to.deep.equal([
          [10, 10],
          [11, 11],
          [13, 13],
          [15, 15],
          [16, 16],
        ]);
      });
    })
  })


  describe("garbage collect", function() {
    it("iterates over all pointers", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [13],
        children: [{
          keys: [9, 11],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }, {
            keys: [11, 12],
            values: [11, 12],
          }],
        }, {
          keys: [16],
          children: [{
            keys: [13, 15],
            values: [13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.mark(ptr => {
          results.push(ptr);
          return true;
        });
      }).then(() => {
        expect(results).to.deep.equal([
          1008,
          1006,
          1001,
          1002,
          1003,
          1007,
          1004,
          1005,
        ]);
      });
    })

    it("skips uninteresting sub-trees", function() {
      let results = [];
      return deserializeTree(store, {
        keys: [13],
        children: [{
          keys: [9, 11],
          children: [{
            keys: [1, 4],
            values: [1, 4],
          }, {
            keys: [9, 10],
            values: [9, 10],
          }, {
            keys: [11, 12],
            values: [11, 12],
          }],
        }, {
          keys: [16],
          children: [{
            keys: [13, 15],
            values: [13, 15],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }],
      }).then(ptr => {
        return new AsyncTree({ store }, ptr);
      }).then(tree => {
        return tree.mark(ptr => {
          results.push(ptr);
          return ptr !== 1001;
        });
      }).then(() => {
        expect(results).to.deep.equal([
          1008,
          1006,
          1001,
          1007,
          1004,
          1005,
        ]);
      });
    })
  })
})
