"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sinon = require("sinon");

const { Replica } = require("..");
const { Tree } = require("../..");
const { TestStore } = require("../../tests/teststore");

const has = Object.prototype.hasOwnProperty;

describe("Replica", function() {
  beforeEach(function() {
    this.config = {
      bulkSize: 100,
      tables: [{
        schema: "public",
        name: "employee",
        columns: {
          id: {},
          first_name: {},
          last_name: {},
          occupation: {},
        },
        indices: [{
          keyPath: ["id"],
        }, {
          name: "name",
          unique: false,
          keyPath: ["last_name", "first_name"],
        }],
      }, {
        schema: "public",
        name: "project",
        indices: [{
          keyPath: ["id"],
        }],
      }],
    };

    this.store = new TestStore();
    this.tree = Tree.empty(this.store);
    this.replica = new Replica(this.tree, this.config);

    let rows = [
      [[0, 0, 1], { id: 1, first_name: "Albert", last_name: "Einstein", occupation: "Physicist" }],
      [[0, 0, 2], { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Mathematician" }],
      [[0, 0, 3], { id: 3, first_name: "Rosalind", last_name: "Franklin", occupation: "Chemist" }],
      [[0, 1, 1], { id: 1, first_name: "Albert", last_name: "Einstein", occupation: "Physicist" }],
      [[0, 1, 2], { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Mathematician" }],
      [[0, 1, 3], { id: 3, first_name: "Rosalind", last_name: "Franklin", occupation: "Chemist" }],
      [[1, 0, 1], { id: 1, name: "R&D" }],
      [[1, 0, 2], { id: 2, name: "Mining" }],
    ];
    return this.tree.bulk(rows);
  });

  it("writes index on commit", function() {
    return this.replica.onEvent({
      type: "COMMIT",
      tx: "txname",
    }).then(() => {
      expect(this.replica.indexName).to.equal("txname");
    });
  })

  it("handles row update", function() {
    return this.replica.onEvent({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      row: { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    }).then(() => {
      return this.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })

  it("handles row update, ignoring extra column", function() {
    return this.replica.onEvent({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      row: { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber", extra: "ignored" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    }).then(() => {
      return this.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })

  it("handles row insert", function() {
    return this.replica.onEvent({
      type: "INSERT",
      schema: "public",
      name: "employee",
      row: { id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    }).then(() => {
      return this.tree.get([0, 0, 4]);
    }).then(value => {
      expect(value).to.deep.equal({ id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" });
    });
  })

  it("handles row insert, ignoring extra column", function() {
    return this.replica.onEvent({
      type: "INSERT",
      schema: "public",
      name: "employee",
      row: { id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist", extra: "ignored" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    }).then(() => {
      return this.tree.get([0, 0, 4]);
    }).then(value => {
      expect(value).to.deep.equal({ id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" });
    });
  })

  it("handles row delete", function() {
    return this.replica.onEvent({
      type: "DELETE",
      schema: "public",
      name: "employee",
      row: { id: 3 },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    }).then(() => {
      return this.tree.get([0, 0, 3]);
    }).then(value => {
      expect(value).to.be.undefined;
    });
  })

  it("automatically does bulk operation after buffering enough rows", function() {
    let promise = Promise.resolve();
    for (let i = 0; i < this.config.bulkSize * 2 - 2; ++i) {
      promise = promise.then(() => this.replica.onEvent({
        type: "UPDATE",
        schema: "public",
        name: "project",
        row: { id: 2, name: i },
      }));
    }
    return promise.then(() => {
      return this.tree.get([1, 0, 2]);
    }).then(value => {
      expect(value.name).to.equal(this.config.bulkSize - 1);
    });
  })

  it("ignores unknown event type", function() {
    return this.replica.onEvent({
      type: "IGNORED",
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    });
  })

  it("ignores unknown schema", function() {
    return this.replica.onEvent({
      type: "INSERT",
      schema: "unknown",
      name: "employee",
      row: { foo: "bar" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    });
  })

  it("ignores unknown table", function() {
    return this.replica.onEvent({
      type: "INSERT",
      schema: "public",
      name: "unknown",
      row: { foo: "bar" },
    }).then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "1",
      });
    });
  })
})
