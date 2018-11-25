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
  
  if (!has.call(node, "children$")) {
    store.write(node);
    return Promise.resolve(node[PTR]);
  }

  let children = json.children$.map(c => deserializeTree(store, c));
  return Promise.all(children).then(children => {
    node.children$ = children;
    store.write(node);
    return Promise.resolve(node[PTR]);
  });
}

const serializeTree = (store, ptr) => {
  return store.read(ptr).then(tree => {
    let json = Object.assign({}, tree);
    delete json.id;
    
    if (!has.call(json, "children$"))
      return json;

    let children = json.children$.map(c => serializeTree(store, c));
    return Promise.all(children).then(children => {
      json.children$ = children;
      return json;
    });
  });
}

const testStoreFactory = () => {
  return Promise.resolve(new TestStore());
}

testStoreFactory.after = (store) => {
  return Promise.resolve();
}

const fileStoreFactory = (path=TEMP_DIR) => {
  sh.mkdir("-p", path);
  return FileStore.create(path);
}

fileStoreFactory.after = (store) => {
  return store.flush().then(() => {
    sh.rm("-rf", join(TEMP_DIR, "*"));
  });
}

[fileStoreFactory, testStoreFactory].forEach(factory => {
  describe("Tree", function() {
    beforeEach(function() {
      this.sandbox = sinon.sandbox.create();
      return factory().then(store => {
        this.store = store;
      });
    })

    afterEach(function() {
      this.timeout(60000);
      this.sandbox.restore();
      return factory.after(this.store);
    })

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
          children$: [{
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
              children$: [{
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


    describe("lifetime", function() {
      it("has no entries on creation", function() {
        let tree = Tree.empty(this.store);
        return serializeTree(this.store, tree.rootPtr).then(tree => {
          expect(tree).to.deep.equal({
            keys: [],
            values: [],
          });
        });
      })

      it("can clone tree, marking all target nodes", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(1, 10).then(() => {
          return tree.clone({
            mark: true,
          });
        }).then(clone => {
          return clone.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      });

      it("can clone tree to new store", function() {
        let tree = Tree.empty(this.store);
        return factory(join(TEMP_DIR, "store2")).then(store2 => {
          return tree.insert(1, 10).then(() => {
            return tree.clone({ store: store2 });
          }).then(clone => {
              return clone.get(1);
          }).then(value => {
            expect(value).to.equal(10);
          });
        });
      });

      it("can clone tree to new store without linking nodes", function() {
        let tree = Tree.empty(this.store);
        return factory(join(TEMP_DIR, "store2")).then(store2 => {
          return tree.insert(1, 10).then(() => {
            return tree.clone({
              store: store2,
              tryLink: false,
            });
          }).then(clone => {
              return clone.get(1);
          }).then(value => {
            expect(value).to.equal(10);
          });
        });
      });
    })

    describe("set", function() {
      it("after creation", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(1, 10).then(value => {
          expect(value).to.equal(undefined);
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [1],
            values: [10],
          });
        });
      })

      it("entries are maintained in ascending in node", function() {
        let tree = Tree.empty(this.store);
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
        let tree = Tree.empty(this.store);
        return tree.insert(1, 30).then(() => {
          return tree.update(1, 10);
        }).then(value => {
          expect(value).to.equal(30);
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
        let tree = Tree.empty(this.store);
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
        let tree = Tree.empty(this.store);
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
          children$: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 25],
            values: [16, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(20, 20).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [16],
              children$: [{
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
          children$: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 20, 25],
            values: [16, 20, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(13, 13).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [9, 16],
              children$: [{
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
          children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(15, 15).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [9, 16],
              children$: [{
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
          children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(10, 10).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [9, 13, 16],
              children$: [{
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
          children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(11, 11).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [9, 13, 16],
              children$: [{
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
          children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.insert(12, 12).then(() => {
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [13],
              children$: [{
                keys: [9, 11],
                children$: [{
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
                children$: [{
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
        let tree = Tree.empty(this.store);
        return tree.insert(1, 10).then(() => {
          return tree.delete(1);
        }).then(value => {
          expect(value).to.equal(10);
          return serializeTree(this.store, tree.rootPtr);
        }).then(tree => {
          expect(tree).to.deep.equal({
            keys: [],
            values: [],
          });
        });
      })
      
      it("undefined result if does not exist", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(1, 10).then(() => {
          return tree.delete(2).then(value => {
            expect(value).to.be.undefined;
            return tree.get(1).then(value => {
              expect(value).to.equal(10);
            });
          });
        });
      })

      it("13", function() {
        return deserializeTree(this.store, {
          keys: [13],
          children$: [{
            keys: [9, 11],
            children$: [{
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
            children$: [{
              keys: [13, 15],
              values: [13, 15],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.delete(13).then(value => {
            expect(value).to.equal(13);
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [13],
              children$: [{
                keys: [9, 11],
                children$: [{
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
                children$: [{
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
          children$: [{
            keys: [9, 11],
            children$: [{
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
            children$: [{
              keys: [15, 16],
              values: [15, 16],
            }, {
              keys: [20, 25],
              values: [20, 25],
            }],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.delete(15).then(value => {
            expect(value).to.equal(15);
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [11],
              children$: [{
                keys: [9],
                children$: [{
                  keys: [1, 4],
                  values: [1, 4],
                }, {
                  keys: [9, 10],
                  values: [9, 10],
                }],
              }, {
                keys: [13],
                children$: [{
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
          children$: [{
            keys: [9],
            children$: [{
              keys: [1, 4],
              values: [1, 4],
            }, {
              keys: [9, 10],
              values: [9, 10],
            }],
          }, {
            keys: [13],
            children$: [{
              keys: [11, 12],
              values: [11, 12],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.delete(1).then(value => {
            expect(value).to.equal(1);
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [11, 13],
              children$: [{
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
          children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.delete(20).then(value => {
            expect(value).to.equal(20);
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [9, 15],
              children$: [{
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
          children$: [{
            keys: [9],
            children$: [{
              keys: [1, 4],
              values: [1, 4],
            }, {
              keys: [9, 10],
              values: [9, 10],
            }],
          }, {
            keys: [15, 20],
            children$: [{
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
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
          return tree.delete(4).then(value => {
            expect(value).to.equal(4);
            return serializeTree(this.store, tree.rootPtr);
          }).then(tree => {
            expect(tree).to.deep.equal({
              keys: [15],
              children$: [{
                keys: [11],
                children$: [{
                  keys: [1, 9, 10],
                  values: [1, 9, 10],
                }, {
                  keys: [11, 12],
                  values: [11, 12],
                }],
              }, {
                keys: [20],
                children$: [{
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
          children$: [{
            keys: [1, 4, 9],
            values: [1, 4, 9],
          }, {
            keys: [16, 25],
            values: [16, 25],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, {
            rootPtr$: ptr,
            config: { order: 2 }
          });
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
              children$: [{
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
        let tree = Tree.empty(this.store);
        let results = [];
        return tree.insert(2, 20).then(() => {
          return tree.get(2);
        }).then(value => {
          expect(value).to.equal(20);
        });
      })

      it("gets returns undefined for missing record", function() {
        let tree = Tree.empty(this.store);
        let results = [];
        return tree.insert(2, 20).then(() => {
          return tree.get(3);
        }).then(value => {
          expect(value).to.be.undefined;
        });
      })

      it("finds only record", function() {
        let tree = Tree.empty(this.store);
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
        let tree = Tree.empty(this.store);
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
          children$: [{
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
          let tree = new Tree(this.store, { rootPtr$: ptr });
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
          children$: [{
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
          let tree = new Tree(this.store, { rootPtr$: ptr });
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
          children$: [{
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
          let tree = new Tree(this.store, { rootPtr$: ptr });
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
          children$: [{
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
          let tree = new Tree(this.store, { rootPtr$: ptr });
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

    describe("forEachPtr", function() {
      it("iterates over all pointers", function() {
        let results = [];
        return deserializeTree(this.store, {
          keys: [13],
          children$: [{
            keys: [9, 11],
            children$: [{
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
            children$: [{
              keys: [13, 15],
              values: [13, 15],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, { rootPtr$: ptr });
          return tree.forEachPtr(ptr => {
            results.push(ptr);
          });
        }).then(height => {
          expect(height).to.equal(3);
          expect(results.length).to.equal(8);
        });
      })

      it("skips nodes if callback returns true", function() {
        let results = [];
        return deserializeTree(this.store, {
          keys: [13],
          children$: [{
            keys: [9, 11],
            children$: [{
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
            children$: [{
              keys: [13, 15],
              values: [13, 15],
            }, {
              keys: [16, 20, 25],
              values: [16, 20, 25],
            }],
          }],
        }).then(ptr => {
          let tree = new Tree(this.store, { rootPtr$: ptr });
          return tree.forEachPtr((ptr, depth) => {
            results.push(ptr);
            return depth > 0;
          });
        }).then(height => {
          expect(height).to.be.undefined;
          expect(results.length).to.equal(3);
        });
      })
    })

    describe("fuzz", function() {
      const FUZZ_SIZE = 20000;

      it("bulk insert", function(done) {
        let tree = Tree.empty(this.store);

        let i = 0;

        const doRandom = () => {
          i += 1000;
          if (i % 1000 === 0)
            console.log(i);

          let changes = [];
          for (let j = 0; j < 1000; ++j) {
            let key = Math.random().toString(16).substring(2, 6);
            let value = Math.random().toString(36).substring(2);
            changes.push([key, value]);
          }
          let promise = tree.bulk(changes);

          return promise.catch(() => {}).then(() => {
            if (i >= FUZZ_SIZE) {
              return this.store.flush().then(() => {
                done();
              });
            }

            return doRandom();
          });
        }

        doRandom();
      }).timeout(600000);
      
      it("insert and delete", function(done) {
        let tree = Tree.empty(this.store);

        let i = 0;

        const doRandom = () => {
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

          if (i % 10000 === 0)
            promise = promise.then(() => tree.commit());

          return promise.catch(() => {}).then(() => {
            if (i >= FUZZ_SIZE) {
              return this.store.flush().then(() => {
                done();
              });
            } else {
              return doRandom();
            }
          });
        }

        doRandom();
      }).timeout(600000);
    })

    describe("transaction", function() {
      it("writes committed tree", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(2, 20).then(() => {
          return tree.insert(1, 10);
        }).then(() => {
          return this.store.writeMeta(meta => {
            meta.myTree = tree.commit();
          });
        }).then(() => {
          return this.store.readMeta();
        }).then(meta => {
          let tree2 = new Tree(this.store, meta.myTree);
          return tree2.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      })

      it("can rollback operations since last commit", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(1, 10).then(() => {
          tree.commit();
          return tree.update(1, 20);
        }).then(() => {
          tree.rollback();
          return tree.get(1);
        }).then(value => {
          expect(value).to.equal(10);
        });
      })

      it("garbage collects tree in store", function() {
        let tree = Tree.empty(this.store);
        return tree.insert(2, 20).then(() => {
          return tree.insert(1, 10);
        }).then(() => {
          return this.store.writeMeta(meta => {
            meta.myTree = tree.commit();
          });
        }).then(() => {
          return Tree.garbageCollect(this.store, (meta, mark) => {
            return mark(meta.myTree);
          });
        });
      });
    })
  })
})

describe("key order", function() {
  beforeEach(function() {
    this.cmp = Tree.prototype.cmp.bind(Tree.prototype);
  });

  it("orders booleans", function() {
    expect(this.cmp(false, true)).to.equal(-1);
    expect(this.cmp(true, true)).to.equal(0);
    expect(this.cmp(true, false)).to.equal(1);
  })

  it("orders numbers", function() {
    expect(this.cmp(1, 2)).to.equal(-1);
    expect(this.cmp(1, 1)).to.equal(0);
    expect(this.cmp(1, 0)).to.equal(1);
  })

  it("orders strings", function() {
    expect(this.cmp("b", "ba")).to.equal(-1);
    expect(this.cmp("b", "b")).to.equal(0);
    expect(this.cmp("b", "a")).to.equal(1);
  })

  it("orders arrays", function() {
    expect(this.cmp([3], [2])).to.equal(1);
    expect(this.cmp([2], [3])).to.equal(-1);
    expect(this.cmp([2], [2])).to.equal(0);
    expect(this.cmp([2, 0], [2])).to.equal(1);
    expect(this.cmp([2], [2, 0])).to.equal(-1);
  })

  it("orders objects", function() {
  })

  it("orders nulls", function() {
    expect(this.cmp(null, null)).to.equal(0);
  })

  it("boolean before number", function() {
    expect(this.cmp(true, -1000)).to.equal(-1);
    expect(this.cmp(-1000, true)).to.equal(1);
  })

  it("number before string", function() {
    expect(this.cmp(1000, "")).to.equal(-1);
    expect(this.cmp("", 1000)).to.equal(1);
  })

  it("string before array", function() {
    expect(this.cmp("", [])).to.equal(-1);
    expect(this.cmp([], "")).to.equal(1);
  })

  it("array before object", function() {
    expect(this.cmp({b: 0}, {a: 0, b: 0})).to.equal(-1);
    expect(this.cmp({a: 0, b: 0}, {b: 0})).to.equal(1);

    expect(this.cmp({a: 0}, {b: 0})).to.equal(-1);
    expect(this.cmp({b: 0}, {b: 0})).to.equal(0);
    expect(this.cmp({c: 0}, {b: 0})).to.equal(1);

    expect(this.cmp({b: 0}, {a: 0})).to.equal(1);
    expect(this.cmp({b: 0}, {b: 0})).to.equal(0);
    expect(this.cmp({b: 0}, {c: 0})).to.equal(-1);

    expect(this.cmp({b: 0}, {b: 1})).to.equal(-1);
    expect(this.cmp({b: 1}, {b: 1})).to.equal(0);
    expect(this.cmp({b: 2}, {b: 1})).to.equal(1);

    expect(this.cmp({b: 1}, {b: 0})).to.equal(1);
    expect(this.cmp({b: 1}, {b: 1})).to.equal(0);
    expect(this.cmp({b: 1}, {b: 2})).to.equal(-1);
  })

  it("object before null", function() {
    expect(this.cmp({}, null)).to.equal(-1);
    expect(this.cmp(null, {})).to.equal(1);
  })


  it("array of boolean before array of number", function() {
    expect(this.cmp([false], [1])).to.equal(-1);
    expect(this.cmp([1], [false])).to.equal(1);
  })

  it("array of number before array of string", function() {
    expect(this.cmp([1], [""])).to.equal(-1);
    expect(this.cmp([""], [1])).to.equal(1);
  })

  it("array of string before array of array", function() {
    expect(this.cmp(["a"], [[]])).to.equal(-1);
    expect(this.cmp([[]], ["a"])).to.equal(1);
  })

  it("array of array before array of object", function() {
    expect(this.cmp([[]], [{}])).to.equal(-1);
    expect(this.cmp([{}], [[]])).to.equal(1);
  })

  it("array of object before array of null", function() {
    expect(this.cmp([{}], [null])).to.equal(-1);
    expect(this.cmp([null], [{}])).to.equal(1);
  })

  it("short array before longer array", function() {
    expect(this.cmp([[]], [[], []])).to.equal(-1);
    expect(this.cmp([[], []], [[]])).to.equal(1);
  })


  it("object of boolean before object of number", function() {
    expect(this.cmp({ a: false }, { a: 1 })).to.equal(-1);
    expect(this.cmp({ a: 1 }, { a: false })).to.equal(1);
  })

  it("object of number before object of string", function() {
    expect(this.cmp({ a: 1 }, { a: "" })).to.equal(-1);
    expect(this.cmp({ a: "" }, { a: 1 })).to.equal(1);
  })

  it("object of string before object of array", function() {
    expect(this.cmp({ a: "a" }, { a: {} })).to.equal(-1);
    expect(this.cmp({ a: {} }, { a: "a" })).to.equal(1);
  })

  it("object of array before object of object", function() {
    expect(this.cmp({ a: [] }, { a: {} })).to.equal(-1);
    expect(this.cmp({ a: {} }, { a: [] })).to.equal(1);
  })

  it("object of object before object of null", function() {
    expect(this.cmp({ a: {} }, { a: null })).to.equal(-1);
    expect(this.cmp({ a: null }, { a: {} })).to.equal(1);
  })

  it("short array before longer array", function() {
    expect(this.cmp({ a: {} }, { a: {}, b: {} })).to.equal(-1);
    expect(this.cmp({ a: {}, b: {} }, { a: {} })).to.equal(1);
  })
})
