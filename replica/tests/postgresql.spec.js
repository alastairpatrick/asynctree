"use strict";

const { expect } = require("chai");
const { Client } = require("pg");
const sinon = require("sinon");

const { PGPublisher } = require("../postgresql");
const { TestStore } = require("../../tests/teststore");

const has = Object.prototype.hasOwnProperty;

const readAll = (cursor) => {
  let result = [];
  const readRows = () => {
    return cursor.read().then(rows => {
      if (rows.length === 0)
        return;
      result = result.concat(rows);
      return readRows();
    });
  }
  return readRows().then(() => result);
}

// Need to have a database configured to run these tests so they are disabled.
xdescribe("Publisher", function() {
  beforeEach(function() {
    this.client = new Client({
      connectionString: "postgres://postgres@localhost:5432/zeta",
    });
    this.client.connect();
    this.store = new TestStore();
    this.config = {
      tables: [],
    }
    this.publisher = new PGPublisher(this.store, this.config, this.client);
  })

  afterEach(function() {
    return this.publisher.end();
  });

  it("queries rows in table", function() {
    let cursor = this.publisher.query({
      "schema": "pg_catalog",
      "name": "pg_class",
    }, ["oid"]);

    return readAll(cursor).then(result => {
      expect(result.length).to.be.greaterThan(0);
    });
  });

  it("query rows of table in snapshot transaction", function() {
    return this.publisher.snapshot().then(() => {
      let cursor = this.publisher.query({
        "schema": "pg_catalog",
        "name": "pg_class",
      }, ["oid"]);

      return readAll(cursor);
    }).then(result => {
      expect(result.length).to.be.greaterThan(0);
    });
  })
})

