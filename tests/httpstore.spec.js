"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { HttpStore } = require("../httpstore");
const { PTR } = require("..");

const has = Object.prototype.hasOwnProperty;

describe("HttpStore", function() {
  beforeEach(function() {
    this.sandbox = sinon.sandbox.create();
    if (has.call(global, "XMLHttpRequest"))
      throw new Error("XMLHttpRequest already exists");
    global.XMLHttpRequest = this.sandbox.useFakeXMLHttpRequest();

    this.requests = [];
    global.XMLHttpRequest.onCreate = (request) => {
      this.requests.push(request);
    };

    this.store = new HttpStore("/base/");
    this.store.cacheSize = Infinity;
  })

  afterEach(function() {
    delete global.XMLHttpRequest;
    this.sandbox.restore();
  })

  it("reads by opening HTTP request", function() {
    let promise = this.store.read("000000/000001");
    this.requests[0].respond(200, { "Content-Type": "application/json" }, '{ "keys": [1], "values": [10] }');

    return promise.then(node => {
      expect(this.requests.length).to.equal(1);
      expect(this.requests[0].method).to.equal("GET");
      expect(this.requests[0].url).to.equal("/base/000000/000001");
      expect(node).to.deep.equal({
        keys: [1],
        values: [10],
      });
      expect(node[PTR]).to.equal("000000/000001");
    });
  })

  it("read throws if request returns error status", function() {
    let promise = this.store.read("000000/000001");
    this.requests[0].respond(404, { "Content-Type": "text/plain" }, "Node does not exist");

    return promise.then(() => {
      expect.fail("Did not throw");
    }).catch(error => {
      expect(error).to.match(/Not Found/);
    });
  })

  it("caches node", function() {
    let promise = this.store.read("000000/000001");
    this.requests[0].respond(200, { "Content-Type": "application/json" }, '{ "keys": [1], "values": [10] }');

    return promise.then(node => {
      return this.store.read("000000/000001");
    }).then(node => {
      expect(this.requests.length).to.equal(1);
      expect(node).to.deep.equal({
        keys: [1],
        values: [10],
      });
    });
  })

  it("evicts node when cache full", function() {
    this.store.cacheSize = 1;

    let promise1 = this.store.read("000000/000000");
    this.requests[0].respond(200, { "Content-Type": "application/json" }, '{ "keys": [1], "values": [10] }');
    let promise2 = this.store.read("000000/000001");
    this.requests[1].respond(200, { "Content-Type": "application/json" }, '{ "keys": [2], "values": [20] }');

    return Promise.all([promise1, promise2]).then(([node1, node2]) => {
      expect(this.store.cache.size).to.equal(1);
      expect(this.store.cache.has("000000/000001")).to.be.true;
    });
  })

  it("evicts all nodes after timeout", function(done) {
    this.store.cacheTimeout = 0;

    let promise1 = this.store.read("000000/000000");
    this.requests[0].respond(200, { "Content-Type": "application/json" }, '{ "keys": [1], "values": [10] }');
    let promise2 = this.store.read("000000/000001");
    this.requests[1].respond(200, { "Content-Type": "application/json" }, '{ "keys": [2], "values": [20] }');

    Promise.all([promise1, promise2]).then(([node1, node2]) => {
      expect(this.store.cache.size).to.equal(2);
      expect(this.store.cache.has("000000/000000")).to.be.true;
      expect(this.store.cache.has("000000/000001")).to.be.true;
      setTimeout(() => {
        expect(this.store.cache.size).to.equal(0);
        done();
      }, 1);
    }).catch(error => {
      expect.fail(error);
      done();
    });
  })

  it("reads index", function() {
    let promise = this.store.readMeta("index");
    this.requests[0].respond(200, { "Content-Type": "application/json" }, '{ "rootPtr": "000000/000000" }');
    promise.then(index => {
      expect(index).to.deep.equal({ rootPtr: "000000/000000" });      
    });
  });
})
