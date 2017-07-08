"use strict";

const PTR = Symbol("PTR");

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

module.exports = {
  PTR,
  cloneNode,
};
