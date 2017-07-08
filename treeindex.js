"use strict";

const { PTR } = require("./base");
const { Tree } = require("./tree");

const has = Object.prototype.hasOwnProperty;

const writeEmpty = (store) => {
  let root = {
    keys: [],
    values: [],
  };
  store.write(root);
  return root[PTR];
}

class TreeIndex {
  static open(store, indexPath="index", config={}, TreeClass=Tree) {
    return store.readMeta(indexPath).then(index => {
      let rootPtr;
      if (index === undefined)
        rootPtr = writeEmpty(store);
      else
        rootPtr = index.rootPtr;
      return new TreeIndex(store, indexPath, rootPtr, config, TreeClass);
    });
  }
  
  constructor(store, indexPath, rootPtr, config, TreeClass) {
    this.store = store;
    this.indexPath = indexPath;
    this.trees = new Tree(this.store, rootPtr);
    this.config = config;
    this.TreeClass = TreeClass;
  }

  empty(config={}) {
    config = Object.assign({}, this.config, config);
    let rootPtr = writeEmpty(this.store);
    return new this.TreeClass(this.store, rootPtr, config);
  }

  open(name) {
    return this.trees.get(name).then(tree => {
      if (tree === undefined)
        throw new Error(`Tree '${name}' does not exist.`);
      return new this.TreeClass(this.store, tree.rootPtr, tree.config);
    });
  }

  commit(changes) {
    let trees = Object.assign({}, this.trees);
    let bulk = [];
    for (let name in changes) {
      if (has.call(changes, name)) {
        let tree = changes[name];
        if (tree === undefined) {
          bulk.push([name]);
        } else {
          tree.commit();
          bulk.push([name, {
            config: tree.config,
            rootPtr: tree.rootPtr,
          }]);
        }
      }
    }

    return this.trees.bulk(bulk).then(() => {
      this.trees.commit();
      let index = {
        rootPtr: this.trees.rootPtr,
      };
      return this.store.writeMeta(this.indexPath, index);
    });
  }
}

module.exports = {
  TreeIndex,
}
