"use strict";

const { join } = require("path");
const sh = require("shelljs");

const { Tree, PTR, Transaction, cloneNode } = require(".");
const { FileStore } = require("./filestore");

const TEMP_DIR = join(__dirname, "tests/temp");

const has = Object.prototype.hasOwnProperty;

sh.rm("-rf", join(TEMP_DIR, "*"));

let store = new FileStore(TEMP_DIR);

let emptyNode = {
  keys: [],
  values: [],
};
store.write(emptyNode);

let tree = new Tree(store, emptyNode[PTR], {
  order: 500,
});

let i = 0;

const doRandom = () => {
  ++i;
  if (i % 1000 === 0)
    console.log(i);

  let key = Math.random().toString(16).substring(2, 6);
  let value = Math.random().toString(36).substring(2);

  let promise;
  promise = tree.set(key, value);

  return promise.catch(error => {
    expect.fail(error);
  }).then(() => {
    if (i >= 50000) {
      return store.flush();
    } else if (i % 100 === 0) {
      setTimeout(doRandom, 0);
    } else {
      return doRandom();
    }
  });
}

doRandom().then(() => {
  console.log("Done");
});
