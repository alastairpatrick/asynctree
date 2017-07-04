"use strict";

const { PTR } = require("./base");
const { Tree } = require("./tree");

const has = Object.prototype.hasOwnProperty;

class TreeIndex {
  static open(store, config={}, TreeClass=Tree) {
    return store.readTreeIndex().then(trees => {
      return new TreeIndex(store, trees, config, TreeClass);
    });
  }

  constructor(store, trees, config, TreeClass) {
    this.store = store;
    this.trees = trees;
    this.config = config;
    this.TreeClass = TreeClass;
  }

  empty(config={}) {
    config = Object.assign({}, this.config, config);

    let root = {
      keys: [],
      values: [],
    };
    this.store.beginWrite(root);
    this.store.endWrite(root);

    return new this.TreeClass(this.store, root[PTR], config);
  }

  open(name) {
    if (!has.call(this.trees, name))
      return Promise.reject(`Tree '${name}' does not exist.`);

    let tree = this.trees[name];
    return Promise.resolve(new this.TreeClass(this.store, tree.rootPtr, tree.config));
  }

  commit(changes) {
    let trees = Object.assign({}, this.trees);
    for (let name in changes) {
      if (has.call(changes, name)) {
        let tree = changes[name];
        if (tree === undefined) {
          delete trees[name];
        } else {
          tree.commit();
          trees[name] = {
            config: tree.config,
            rootPtr: tree.rootPtr,
          };
        }
      }
    }

    return this.store.flush().then(() => {
      return this.store.writeTreeIndex(trees);
    }).then(() => {
      this.trees = trees;
    });
  }
}

module.exports = {
  TreeIndex,
}
