"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sh = require("shelljs");
const sinon = require("sinon");

const { Tree, PTR, Transaction, cloneNode } = require("..");
const { FileStore } = require("../filestore");
const { TestStore } = require("./teststore");

const TEMP_DIR = join(__dirname, "temp");

const has = Object.prototype.hasOwnProperty;


const deserializeTree = (store, json) => {
  let node = Object.assign({}, json);
  
  if (!has.call(node, "children")) {
    store.write(node);
    return Promise.resolve(node[PTR]);
  }

  let children = json.children.map(c => deserializeTree(store, c));
  return Promise.all(children).then(children => {
    node.children = children;
    store.write(node);
    return Promise.resolve(node[PTR]);
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

const testStoreFactory = () => {
  return new TestStore();
}

testStoreFactory.after = (store) => {
  return Promise.resolve();
}

const fileStoreFactory = () => {
  return new FileStore(TEMP_DIR);
}

fileStoreFactory.after = (store) => {
  return store.flush().then(() => {
    sh.rm("-rf", join(TEMP_DIR, "*"));
  });
}

[testStoreFactory, fileStoreFactory].forEach(factory => {
  describe("Tree", function() {
    beforeEach(function() {
      this.sandbox = sinon.sandbox.create();
      this.store = factory();
      let emptyNode = {
        keys: [],
        values: [],
      };
      this.store.write(emptyNode);
      this.emptyNodePtr = emptyNode[PTR];
    })

    afterEach(function() {
      this.sandbox.restore();
      return factory.after(this.store);
    });

    describe("round trips", function() {
      it("unit height tree", function() {
        return deserializeTree(this.store, {
          keys: [1, 2],
          values: [10, 20],
        }).then(ptr => {
          return serializeTree(this.store, ptr).then(tree => {
            expect(tree).to.deep.equal({
              keys: [1, 2],
              values: [10, 20],
            });
          });
        });
      })

      it("tree with internal nodes", function() {
        return deserializeTree(this.store, {
          keys: [2],
          children: [{
            keys: [1],
            values: [10],
          }, {
            keys: [2],
            values: [20],
          }],
        }).then(ptr => {
          return serializeTree(this.store, ptr).then(tree => {
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
      let tree = new Tree(this.store, this.emptyNodePtr);
      return serializeTree(this.store, tree.rootPtr).then(tree => {
        expect(tree).to.deep.equal({
          keys: [],
          values: [],
        });
      });
    })


    describe("set", function() {
      it("after creation", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(1, 10).then(() => {
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [1],
            values: [10],
          });
        });
      })

      it("entries are maintained in ascending in node", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(3, 30).then(() => {
          return tree.insert(1, 10);
        }).then(() => {
          return tree.insert(2, 20);
        }).then(() => {
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [1, 2, 3],
            values: [10, 20, 30],
          });
        });
      })

      it("can update value associated with key", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(1, 30).then(() => {
          return tree.update(1, 10);
        }).then(() => {
          return tree.insert(2, 20);
        }).then(() => {
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [1, 2],
            values: [10, 20],
          });
        });
      })

      it("exception on attempt to add key that already exists", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(1, 30).then(() => {
          return tree.insert(1, 10).then(() => {
            expect.fail("Did not throw");
          }).catch(error => {
            expect(error).to.match(/'1'/);

            return tree.get(1).then(value => {
              expect(value).to.equal(30);
            });
          });
        });
      })

      it("exception on attempt to update key that does not exist", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(2, 20).then(() => {
          return tree.update(1, 10).then(() => {
            expect.fail("Did not throw");
          }).catch(error => {
            expect(error).to.match(/'1'/);

            return tree.get(2).then(value => {
              expect(value).to.equal(20);
            });
          });
        });
      })

      // Tests identified with numbers here and elsewhere are based on examples taken from notes here:
      // http://www.cburch.com/cs/340/reading/btree/
      it("20", function() {
        return deserializeTree(this.store, {
          keys: [16],
          children: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 25],
            values: [16, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(20, 20).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("13", function() {
        return deserializeTree(this.store, {
          keys: [16],
          children: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(13, 13).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("15", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(15, 15).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("10", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(10, 10).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("11", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(11, 11).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("12", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.insert(12, 12).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })
    })

    describe("delete", function() {
      it("sole value", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(1, 10).then(() => {
          return tree.delete(1);
        }).then(() => {
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [],
            values: [],
          });
        });
      })
      
      it("throws exception if does not exist", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.insert(1, 10).then(() => {
          return tree.delete(2).then(() => {
            expect.fail("Did not throw");
          }).catch(error => {
            expect(error).to.match(/'2'/);

            return tree.get(1).then(value => {
              expect(value).to.equal(10);
            });
          });
        });
      })

      it("13", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.delete(13).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("15", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.delete(15).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })


      it("1", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.delete(1).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("merges into rightmost leaf", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.delete(20).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })

      it("merges into leftmost internal node", function() {
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.delete(4).then(() => {
            return serializeTree(this.store, tree.rootPtr);
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
        });
      })
    })


    describe("bulk", function() {
      it("change", function() {
        return deserializeTree(this.store, {
          keys: [16],
          children: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 25],
            values: [16, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr, { order: 2 });
          return tree.bulk([
            [20, 20],
            [13, 13],
            [15, 15],
            [10, 10],
            [11, 11],
            [12, 12],
            [13],  // was inserted earlier
            [15],  // was inserted earlier
            [1],
          ]).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              children: [{
                  keys: [4, 9],
                  values: [4, 9],
                }, {
                  keys: [10, 11],
                  values: [10, 11],
                }, {
                  keys: [12, 16],
                  values: [12, 16],
                }, {
                  keys: [20, 25],
                  values: [20, 25],
                },
              ],
              keys: [10, 12, 20],
            });
          });
        });
      })
    })

    describe("query", function() {
      it("gets particular record", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        let results = [];
        return tree.insert(2, 20).then(() => {
          return tree.get(2);
        }).then(value => {
          expect(value).to.equal(20);
        });
      })

      it("gets returns undefined for missing record", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        let results = [];
        return tree.insert(2, 20).then(() => {
          return tree.get(3);
        }).then(value => {
          expect(value).to.be.undefined;
        });
      })

      it("finds only record", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        let results = [];
        return tree.insert(2, 20).then(() => {
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
        let tree = new Tree(this.store, this.emptyNodePtr);
        let results = [];
        return tree.insert(2, 20).then(() => {
          return tree.insert(1, 10);
        }).then(() => {
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
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr);
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
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr);
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
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr);
          return tree.forEach((value, key) => {
            results.push([key, value]);
            if (key >= 13)
              return Tree.BREAK;
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
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr);
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
        return deserializeTree(this.store, {
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
          let tree = new Tree(this.store, ptr);
          return tree.mark(ptr => {
            results.push(ptr);
            return true;
          });
        }).then(() => {
          expect(results.length).to.equal(8);
        });
      })
    })

    describe("transaction", function() {
      it("performs action", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.atomically(() => {
          return tree.insert(1, 10);
        }).then(() => {
          return tree.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      })

      it("performs multiple action", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.atomically(() => {
          return tree.insert(1, 10).then(() => {
            return tree.insert(2, 20);
          });
        }).then(() => {
          return tree.get(2);
        }).then(value => {
          expect(value).to.equal(20);
        });
      })

      it("undoes actions on exception", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.atomically(() => {
          return tree.insert(1, 10).then(() => {
            throw new Error("Unexpected thing");
          });
        }).then(() => {
          expect.fail("Did not throw");
        }).catch(error => {
          expect(error).to.match(/Unexpected thing/);
          return tree.get(1);
        }).then(value => {
          expect(value).to.be.undefined;
        });
      })

      it("nests", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.atomically(() => {
          return tree.atomically(() => {
            return tree.insert(1, 10);
          });
        }).then(() => {
          return tree.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      })

      it("undoes inner actions for inner exception", function() {
        let tree = new Tree(this.store, this.emptyNodePtr);
        return tree.atomically(() => {
          return tree.insert(1, 10).then(() => {
            return tree.atomically(() => {
              return tree.insert(2, 20).then(() => {
                throw new Error("Unexpected thing");
              });
            }).catch(error => {
              // Do not propagate error to outer atomically
            });
          });
        }).then(value => {
          expect(value).to.be.undefined;
        }).then(() => {
          return tree.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      })

      it("can rollback tree", function() {
        return deserializeTree(this.store, {
          keys: [],
          values: [],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr);
          return tree.insert(1, 10).then(() => {
            return tree.rollback();
          }).then(() => {
            return tree.get(1);
          }).then(value => {
            expect(value).to.be.undefined;
          });
        });
      })

      it("can commit tree", function() {
        return deserializeTree(this.store, {
          keys: [],
          values: [],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr);
          return tree.insert(1, 10).then(() => {
            return tree.commit();
          }).then(() => {
            return tree.get(1);
          }).then(value => {
            expect(value).to.equal(10);
          });
        });
      })

      it("can commit tree and rollback", function() {
        return deserializeTree(this.store, {
          keys: [],
          values: [],
        }).then(ptr => {
          let tree = new Tree(this.store, ptr);
          return tree.insert(1, 10).then(() => {
            return tree.commit();
          }).then(() => {
            return tree.insert(2, 20);
          }).then(() => {
            tree.rollback();
            return tree.get(1);
          }).then(value => {
            expect(value).to.equal(10);
          });
        });
      })
    })

    describe("fuzz", function() {
      it("insert and delete", function(done) {
        let tree = new Tree(this.store, this.emptyNodePtr);

        let i = 0;
        let id;

        const doRandom = () => {
          if (i >= 10000 && id !== undefined) {
            clearInterval(id);
            id = undefined;
            return this.store.flush().then(() => {
              done();
            });
          }

          ++i;
          if (i % 1000 === 0)
            console.log(i);

          let key = Math.random().toString(16).substring(2, 6);
          let value = Math.random().toString(36).substring(2);

          let promise;
          if (i % 3 === 0)
            promise = tree.delete(key);
          else
            promise = tree.set(key, value);

          return promise.catch(() => {}).then(() => {
            if (i % 100 === 0)
              return;

            return doRandom();
          });
        }

        id = setInterval(doRandom, 0);
        doRandom();
      }).timeout(60000);
    });
  })
})
