"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { HttpStore } = require("../httpstore");

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
})
