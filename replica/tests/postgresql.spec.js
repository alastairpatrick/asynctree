"use strict";

const { expect } = require("chai");
const pg = require("pg");
const sinon = require("sinon");

const { Client } = require("../postgresql");

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
xdescribe("Client", function() {
  beforeEach(function() {
    this.pgClient = new pg.Client({
      connectionString: "postgres://postgres@localhost:5432/zeta",
    });
    this.pgClient.connect();
    this.client = new Client(this.pgClient);
  })

  afterEach(function() {
    return this.client.end();
  });

  it("queries rows in table", function() {
    let cursor = this.client.query({
      "schema": "pg_catalog",
      "name": "pg_class",
    }, ["oid"]);

    return readAll(cursor).then(result => {
      expect(result.length).to.be.greaterThan(0);
    });
  });

  it("query rows of table in snapshot transaction", function() {
    return this.client.snapshot(() => {
      let cursor = this.client.query({
        "schema": "pg_catalog",
        "name": "pg_class",
      }, ["oid"]);

      return readAll(cursor);
    }).then(result => {
      expect(result.length).to.be.greaterThan(0);
    });
  })
})

