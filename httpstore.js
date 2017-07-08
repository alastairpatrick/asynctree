"use strict";

const { PTR } = require("./tree");

const has = Object.prototype.hasOwnProperty;

class HttpStore {
  constructor(prefix) {
    this.prefix = prefix;
    this.cache = new Map();
    this.cacheSize = 12;
    this.timeoutId = undefined;
    this.cacheTimeout = 1000;
  }

  read(ptr) {
    let node = this.cache.get(ptr);
    if (node !== undefined) {
      this.cache_(node);
      return node;
    }

    return this.downloadJSON_(this.prefix + ptr).then(node => {
      node[PTR] = ptr;
      this.cache_(node);
      return node;
    });
  }

  readMeta(path) {
    return this.downloadJSON_(this.prefix + path);
  }

  cache_(node) {
    let ptr = node[PTR];
    this.cache.delete(ptr);
    this.cache.set(ptr, node);

    for (let [ptr, node] of this.cache) {
      if (this.cache.size <= this.cacheSize)
        break;
      this.cache.delete(ptr);
    }

    if (this.timeoutId !== undefined)
      clearTimeout(this.timeoutId);

    this.timeoutId = setTimeout(() => {
      this.timeoutId = undefined;
      this.cache.clear();
    }, this.cacheTimeout);
  }

  downloadJSON_(uri) {
    return new Promise((resolve, reject) => {
      let request = new XMLHttpRequest();
      request.open("GET", uri);
      request.setRequestHeader("Accept", "application/json");
      request.responseType = "json";
      request.onreadystatechange = () => {
        if (request.readyState !== 4)
          return;

        if (request.status === 200)
          resolve(request.response);
        else
          reject(new Error(request.statusText));
      };
      request.onerror = () => {
        reject(new Error(request.statusText));
      };
      request.send();
    });
  }
}

module.exports = {
  HttpStore,
}
