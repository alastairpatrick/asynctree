"use strict";

const PTR = Symbol("PTR");

const read = (store, ptr) => {
  return store.read(ptr);
}

const write = (store, node) => {
  let ptr = store.write(node);
  node[PTR] = ptr;
  return ptr;
}

// When the size of a node is equal to a tree's order, it is the smallest allowable size.
const nodeSize = (node) => {
  if (node.children)
    return node.children.length;
  else
    return node.keys.length;
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
 * Tree nodes need not be stored in memory. Rather, nodes are read asynchronously as needed from a custom
 * backing store. For example, the nodes may be read asynchronously from files or via XMLHttpRequest.
 * Each node contains many entries, making trees relatively shallow.
 * 
 * A tree is immutable and fully persistent, meaning after changes are made, both the modified
 * and original tree are available. Further changes may be made to original tree, creating new
 * modifed trees. Each tree is identified by a pointer to its root node.
 * 
 * Writes to the backing store may be deferred until commit time, preventing uncommited changes
 * from being visible to other transactions.
 * 
 * A pointer identifies a node wrt a backing store. It could be a number or string, such as a
 * filename or URL. Interpretation of pointers is a concern of the backing store, which is
 * provided as configuration when constructing the tree.
 * 
 * Keys and values are considered opaque, except for key comparisons. A comparison function can be confiured,
 * which determines key ordering. The default comparison uses JavaScript's < and > operators
 * and is effective for e.g. strings or numbers (but not both at the same time).
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
      rootPtr = write(this.store, node);
    }

    this.rootPtr = rootPtr;
  }

  /**
   * Clones the tree, optionally changing the root node. No nodes are copied.
   * @param {*} [rootPtr] Pointer to new root node.
   * @returns {AsyncTree} The cloned tree.
   */
  clone(rootPtr=this.rootPtr) {
    return new AsyncTree(this, rootPtr);
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
  set(key, value, operation) {
    let dummyRoot = {
      keys: [],
      children: [this.rootPtr],
    };
    return this.set_(key, value, operation, dummyRoot).then(({ node }) => {
      if (dummyRoot.children.length === 1)
        return this.clone(dummyRoot.children[0]);
      else
        return this.clone(node[PTR]);
    });
  }

  set_(key, value, operation, node) {
    let { idx, cmp } = this.findKey_(key, node);

    if (node.children) {
      return read(this.store, node.children[idx]).then(child => {
        return this.set_(key, value, operation, child);
      }).then(({ node: child, idx: childIdx }) => {
        write(this.store, node);
        node.children[idx] = child[PTR];

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
          write(this.store, sibling);

          node.keys.splice(idx, 0, newKey);
          node.children.splice(idx, 1, child[PTR], sibling[PTR]);
        }

        return { node, idx };
      });
    } else {
      write(this.store, node);
      if (cmp === 0)  {
        if (operation === "insert")
          throw new Error(`Key '${key}' already in tree.`);
        node.values[idx] = value;
      } else {
        if (operation === "update")
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
    return read(this.store, this.rootPtr).then(node => {
      return this.delete_(key, node);
    }).then(({ node, idx }) => {
      if (node.children && node.children.length === 1) {
        return this.clone(node.children[0]);
      } else {
        return this.clone(node[PTR]);
      }
    });
  }

  delete_(key, node) {
    let { idx, cmp } = this.findKey_(key, node);

    if (node.children) {
      return read(this.store, node.children[idx]).then(child => {
        return this.delete_(key, child);
      }).then(({ node: child, idx: childIdx }) => {
        write(this.store, node);
        node.children[idx] = child[PTR];

        if (nodeSize(child) < this.order) {
          let siblingIdx = idx === node.children.length - 1 ? idx - 1 : idx + 1;
          return read(this.store, node.children[siblingIdx]).then(sibling => {
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
              write(this.store, sibling);
              node.children[siblingIdx] = sibling[PTR];

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
              node.children.splice(siblingIdx, 1);
            }

            return { node, idx };
          });
        } else {
          return { node, idx };
        }
      });
    } else {
      if (cmp === 0) {
        write(this.store, node);
        node.keys.splice(idx, 1);
        node.values.splice(idx, 1);
        return Promise.resolve({ node, idx });
      } else {
        return Promise.reject(new Error(`Key '${key}' not found.`));
      }
    }
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
    return read(this.store, this.rootPtr).then(node => {
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
        return read(this.store, node.children[i]).then(child => {
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
}

AsyncTree.BREAK = Symbol("BREAK");

module.exports = {
  AsyncTree,
};
