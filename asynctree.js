"use strict";

const stable = require("stable");

const { PTR } = require("./base");
const { Rollback } = require("./rollback");

const has = Array.prototype.hasOwnProperty;

// TODO: do something else
const storeTransactions = new WeakMap();

const replaceChild = (container, prop, ptr, tx) => {
  let oldPtr = container[prop];
  if (oldPtr !== undefined)
    tx.delete(oldPtr);
  container[prop] = ptr;
}

// When the size of a node is equal to a tree's order, it is the smallest allowable size.
const nodeSize = (node) => {
  if (node.children)
    return node.children.length;
  else
    return node.keys.length;
}

const cloneNode = (node) => {
  let clone = {
    keys: node.keys.slice(),
    [PTR]: node[PTR],
  }
  if (node.values)
    clone.values = node.values.slice();
  if (node.children)
    clone.children = node.children.slice();
  return clone;
}

const cmp = (a, b) => {
  if (a < b)
    return -1;
  else if (a > b)
    return 1;
  else
    return 0;
}

/**
 * A callback invoked for entry in a tree.
 * @callback EachCallback
 * @param {*} value
 * @param {*} key
 */

/**
 * Asynchronous, immutable, persistent multi-way search tree of key-value pairs.
 * 
 * Tree nodes need not be stored in memory. Rather, nodes are read asynchronously as needed from various
 * customizable backing stores. For example, nodes may be read asynchronously from files or via XMLHttpRequest.
 * Each node contains several entries, perhaps 100s or 1000s, making trees relatively shallow.
 * 
 * A tree is immutable and fully persistent, meaning after making changes, both the modified
 * and original tree remain available. Further changes may be made to the original tree, creating new
 * modifed trees.
 * 
 * Writes to the backing store may be deferred until commit, preventing uncommited changes
 * from being visible to other transactions.
 * 
 * Each tree is identified by a pointer to its root node. A pointer identifies a node wrt a backing store.
 * It could be a number or string, such as a filename or URL. It is up to the backing store to interpret 
 * pointers; trees view them as opaque.
 * 
 * Keys and values are also considered opaque, except with regard to key comparison. A comparison function
 * determines key ordering. The default comparison uses JavaScript's < and > operators and is effective for
 * e.g. strings or numbers.
 */
class AsyncTree {
  /**
   * Create a search tree.
   * @param {*} config The confioguration, which must include the backing store for the tree's nodes.
   * @param {*} [rootPtr] Pointer to the tree's root node or undefined to create an empty tree.
   * @returns {AsyncTree} A new tree.
   */
  constructor(config, rootPtr) {
    this.store = config.store;
    this.order = config.order || 1024;
    this.cmp = config.cmp || cmp;

    if (rootPtr === undefined) {
      let node = {
        keys: [],
        values: [],
      };
      this.store.beginWrite(node);
      this.store.endWrite(node);
      rootPtr = node[PTR];
    }

    this.rootPtr = rootPtr;
  }

  atomically_(fn, context) {
    let parent = storeTransactions.get(this.store);
    let rollback = new Rollback(this.store, parent);
    storeTransactions.set(this.store, rollback);

    let rootPtr = this.rootPtr;
    this.rootPtr = undefined;
    try {
      return Promise.resolve(fn.call(context, rollback, rootPtr)).then(rootPtr => {
        this.rootPtr = rootPtr;
        storeTransactions.set(this.store, parent);
        rollback.commit();
      }).catch(error => {
        this.rootPtr = rootPtr;
        storeTransactions.set(this.store, parent);
        rollback.rollback();
        throw error;      
      });
    } catch (error) {
      this.rootPtr = rootPtr;
      storeTransactions.set(this.store, parent);
      rollback.rollback();
      throw error;      
    }
  }

  /**
   * Inserts an entry into the tree, rejecting if the key is already present.
   * @param {*} key The key.
   * @param {*} value Its value.
   * @returns {Promise} Resolves to the new tree.
   */
  insert(key, value) {
    return this.set(key, value, "insert");
  }

  /**
   * Updates the value of an existing value in the tree, rejecting if the key is not present.
   * @param {*} key The key.
   * @param {*} value Its value.
   * @returns {Promise} Resolves to the new tree.
   */
  update(key, value) {
    return this.set(key, value, "update");
  }

  /**
   * Sets the value of a key, inserting a new entry or updating an existing one as necessary.
   * @param {*} key The key.
   * @param {*} value Its value.
   * @returns {Promise} Resolves to the new tree.
   */
  set(key, value, type) {
    return this.atomically_((tx, rootPtr) => this.set_(key, value, type, tx, rootPtr));
  }

  set_(key, value, type, tx, rootPtr) {
    let dummyRoot = {
      keys: [],
      children: [rootPtr],
    };
    return this.setSubTree_(key, value, dummyRoot, type, tx).then(({ node }) => {
      if (dummyRoot.children.length === 1) {
        return dummyRoot.children[0];
      } else {
        tx.beginWrite(node);
        tx.endWrite(node);
        return node[PTR];
      }
    });
  }

  setSubTree_(key, value, node, type, tx) {
    let { idx, cmp } = this.findKey_(key, node);

    if (node.children) {
      return tx.read(node.children[idx]).then(child => {
        return this.setSubTree_(key, value, child, type, tx);
      }).then(({ node: child, idx: childIdx }) => {
        tx.beginWrite(child);
        replaceChild(node.children, idx, child[PTR], tx);

        let sibling, newKey;
        if (child.keys.length >= this.order * 2) {
          if (child.children) {
            sibling = {
              keys: child.keys.slice(this.order + 1),
              children: child.children.slice(this.order + 1),
            };

            newKey = child.keys[this.order];

            child.children = child.children.slice(0, this.order + 1);
          } else {
            sibling = {
              keys: child.keys.slice(this.order),
              values: child.values.slice(this.order),
            };
            newKey = sibling.keys[0];
            
            child.values = child.values.slice(0, this.order);
          }

          child.keys = child.keys.slice(0, this.order);

          node.keys.splice(idx, 0, newKey);

          tx.beginWrite(sibling);
          tx.endWrite(sibling);
          node.children.splice(idx + 1, 0, sibling[PTR]);
        }

        tx.endWrite(child);
        return { node, idx };
      });
    } else {
      if (cmp === 0)  {
        if (type === "insert")
          throw new Error(`Key '${key}' already in tree.`);
        node.values[idx] = value;
      } else {
        if (type === "update")
          throw new Error(`Key '${key}' not found.`);        
        node.keys.splice(idx, 0, key);
        node.values.splice(idx, 0, value);
      }
      return Promise.resolve({ node, idx });
    }
  }

  /**
   * Deletes the entry with the given key, rejecting if the key is not present.
   * @param {*} key The key.
   * @returns {Promise} Resolves to the new tree.
   */
  delete(key) {
    return this.atomically_((tx, rootPtr) => this.rootPtr = this.delete_(key, tx, rootPtr));
  }

  delete_(key, tx, rootPtr) {
    return tx.read(rootPtr).then(node => {
      return this.deleteSubTree_(key, node, tx);
    }).then(({ node, idx }) => {
      if (node.children && node.children.length === 1) {
        return node.children[0];
      } else {
        tx.beginWrite(node);
        tx.endWrite(node);
        return node[PTR];
      }
    });
  }

  deleteSubTree_(key, node, tx) {
    let { idx, cmp } = this.findKey_(key, node);

    if (node.children) {
      return tx.read(node.children[idx]).then(child => {
        return this.deleteSubTree_(key, child, tx);
      }).then(({ node: child, idx: childIdx }) => {
        tx.beginWrite(child);
        replaceChild(node.children, idx, child[PTR], tx);

        if (nodeSize(child) < this.order) {
          let siblingIdx = idx === node.children.length - 1 ? idx - 1 : idx + 1;
          return tx.read(node.children[siblingIdx]).then(sibling => {
            let child1, child2, push, pop, minIdx;
            if (siblingIdx < idx) {
              minIdx = siblingIdx;
              child1 = sibling;
              child2 = child;
              push = Array.prototype.unshift;
              pop = Array.prototype.pop;
            } else {
              minIdx = idx;
              child1 = child;
              child2 = sibling;
              push = Array.prototype.push;
              pop = Array.prototype.shift;
            }

            if (nodeSize(sibling) > this.order) {
              // Child is too small and its sibling is big enough to spare a key so merge it in.
              tx.beginWrite(sibling);
              replaceChild(node.children, siblingIdx, sibling[PTR], tx);

              if (child.children) {
                push.call(child.keys, node.keys[minIdx]);
                node.keys[minIdx] = pop.call(sibling.keys);
                push.call(child.children, pop.call(sibling.children));
              } else {
                if (siblingIdx < idx) {
                  child.keys.unshift(sibling.keys.pop());
                  child.values.unshift(sibling.values.pop());
                } else {
                  child.keys.push(sibling.keys.shift());
                  child.values.push(sibling.values.shift());
                }
                node.keys[minIdx] = child2.keys[0];
              }

              tx.endWrite(sibling);
            } else {
              // Child is too small and its sibling is not big enough to spare any keys so merge them.
              if (child.children) {
                child.keys = child1.keys.concat([node.keys[minIdx]]).concat(child2.keys);
                child.children = child1.children.concat(child2.children);
              } else {
                child.keys = child1.keys.concat(child2.keys);
                child.values = child1.values.concat(child2.values);
              }

              node.keys.splice(minIdx, 1);
              replaceChild(node.children, siblingIdx, undefined, tx);
              node.children.splice(siblingIdx, 1);
            }

            tx.endWrite(child);
            return { node, idx };
          });
        }
        
        tx.endWrite(child);
        return { node, idx };
      });
    } else {
      if (cmp === 0) {
        node.keys.splice(idx, 1);
        node.values.splice(idx, 1);
        return Promise.resolve({ node, idx });
      } else {
        return Promise.reject(new Error(`Key '${key}' not found.`));
      }
    }
  }

  /**
   * Deletes the entry with the given key, rejecting if the key is not present.
   * @param {*} key The key.
   * @returns {Promise} Resolves to the new tree.
   */
  bulk(items) {
    return this.atomically_((tx, rootPtr) => this.bulk_(items, tx, rootPtr));
  }

  bulk_(items, tx, rootPtr) {
    // Sorting is optional but makes node caching by the backing store more effective. Sort must be stable,
    // otherwise operations on the same key could be reordered.
    items = stable(items, (a, b) => this.cmp(a[0], b[0]));
    
    let chain = Promise.resolve(rootPtr);
    items.forEach(item => {
      if (item.length === 1)
        chain = chain.then(rootPtr => this.delete_(item[0], tx, rootPtr));
      else
        chain = chain.then(rootPtr => this.set_(item[0], item[1], item[2], tx, rootPtr));
    });
    return chain;
  }

  /**
   * Gets the value associated with a key.
   * @param {*} key The key.
   * @returns {Promise} Resolves to the value of the entry or undefined if not present.
   */
  get(key) {
    let results = [];
    return this.rangeEach(key, key, value => results.push(value)).then(() => {
      return results[0];
    });
  }

  /**
   * Iterates over every entry in the tree. Enumeration may be terminated early by throwing an exception from
   * the callback function, which will reject the returned promise, or when the callback returns BREAK, which
   * resolves the promise.
   * @param {EachCallback} cb Callback to invoke for each entry.
   * @param {Object} context Value of 'this' for callback.
   * @returns {Promise} Fulfilled after iteration has completed.
   */
  forEach(cb, context) {
    return this.rangeEach(undefined, undefined, cb, context);
  }

  /**
   * Iterates over every entry in the tree. Enumeration may be terminated early by throwing an exception from
   * the callback function, which will reject the returned promise, or when the callback returns BREAK, which
   * resolves the promise.
   * @param {*} lower Key at which to begin iteration.
   * @param {*} upper Key at which to end iteration.
   * @param {EachCallback} cb Callback to invoke for each entry.
   * @param {Object} context Value of 'this' for callback.
   * @returns {Promise} Fulfilled after iteration has completed.
   */
  rangeEach(lower, upper, cb, context) {
    return this.store.read(this.rootPtr).then(node => {
      return this.rangeEach_(lower, upper, cb, context, node);
    });
  }

  rangeEach_(lower, upper, cb, context, node) {
    let i;
    if (lower !== undefined)
      i = this.findKey_(lower, node).idx;
    else
      i = 0;

    if (node.children) {
      const processChildren = (node, i) => {
        return this.store.read(node.children[i]).then(child => {
          return this.rangeEach_(lower, upper, cb, context, child);
        }).then(result => {
          if (result == AsyncTree.BREAK)
            return result;
          ++i;
          if (i >= node.children.length)
            return;
          if (upper !== undefined && i < node.keys.length && this.cmp(node.keys[i], upper) > 0)
            return;
          return processChildren(node, i);
        });
      }

      return processChildren(node, i);
    } else {
      for (; i < node.values.length; ++i) {
        let key = node.keys[i];
        if (this.cmp(key, upper) > 0)
          break;
        if (cb.call(context, node.values[i], key) === AsyncTree.BREAK)
          return AsyncTree.BREAK;
      }
    }
  }

  findKey_(key, node) {
    let idx, cmp;
    if (node.children) {
      for (idx = 0; idx < node.keys.length; ++idx) {
        cmp = this.cmp(node.keys[idx], key);
        if (cmp > 0)
          break;
      }
    } else {
      for (idx = 0; idx < node.keys.length; ++idx) {
        cmp = this.cmp(node.keys[idx], key);
        if (cmp >= 0)
          break;
      }
    }
    return { idx, cmp };
  }

  mark(cb, context) {
    if (!cb.call(context, this.rootPtr))
      return;

    return this.store.read(this.rootPtr).then(node => {
      return this.mark_(cb, context, node);
    });
  }

  mark_(cb, context, node) {
    // This could be optimized by not unnecessarily reading leaf nodes in the first place.
    if (!node.children)
      return;

    const processChildren = (node, i) => {
      let ptr = node.children[i];
      if (!cb.call(context, ptr))
        return;

      return this.store.read(node.children[i]).then(child => {
        return this.mark_(cb, context, child);
      }).then(result => {
        ++i;
        if (i >= node.children.length)
          return;
        return processChildren(node, i);
      });
    }

    return processChildren(node, 0);
  }
}

AsyncTree.BREAK = Symbol("BREAK");

module.exports = {
  AsyncTree,
  PTR,
  cloneNode,
};
