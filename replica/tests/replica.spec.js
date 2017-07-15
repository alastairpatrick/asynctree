"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sinon = require("sinon");

const { Replica } = require("..");
const { Tree } = require("../..");
const { FileStore } = require("../../filestore");
const { TestStore } = require("../../tests/teststore");

const has = Object.prototype.hasOwnProperty;

class TestCursor {
  constructor(table) {
    this.table = table;
    this.idx = 0;
    this.reading = false;
  }

  read() {
    if (this.reading)
      throw new Error("Already reading");

    let rows = this.table.slice(this.idx, this.idx + 100);
    this.idx += rows.length;
    this.reading = true;
    return Promise.resolve(rows).then(rows => {
      this.reading = false;
      return rows;
    });
  }
}

class TestPublisher {
  constructor(tables) {
    this.tables = tables;
    this.events = [];
  }

  snapshot(fn) {
    return Promise.resolve().then(fn);
  }

  query(table) {
    let name = table.schema + "." + table.name;
    return new TestCursor(this.tables[name]);
  }

  stream(handleEvent) {
    if (this.events.length === 0)
      return Promise.resolve();
    
    return Promise.resolve(handleEvent(this.events.shift())).then(() => {
      return this.stream(handleEvent);
    });
  }
}

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

    this.tables = {
      "public.employee": [
        { id: 1, first_name: "Albert", last_name: "Einstein", occupation: "Physicist" },
        { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Mathematician" },
        { id: 3, first_name: "Rosalind", last_name: "Franklin", occupation: "Chemist" },
      ],
      "public.project": [
        { id: 1, name: "R&D" },
        { id: 2, name: "Mining" },
      ],
    };

    this.store = new TestStore();
    this.tree = Tree.empty(this.store);
    this.publisher = new TestPublisher(this.tables);
    this.replica = new Replica(this.tree, this.config);
  });

  it("loads existing rows of each index", function() {
    return this.replica.snapshot(this.publisher).then(() => {
      let result = [];
      return this.tree.forEach((value, key) => {
        result.push([key, value]);
      }).then(() => {
        expect(this.replica.indexName).to.equal("initial");
        expect(result).to.deep.equal([
          [[0, 0, 1], {
            id: 1,
            first_name: "Albert",
            last_name: "Einstein",
            occupation: "Physicist",

          }],
          [[0, 0, 2], {
            id: 2,
            first_name: "Blaise",
            last_name: "Pascal",
            occupation: "Mathematician",
          }],
          [[0, 0, 3], {
            id: 3,
            first_name: "Rosalind",
            last_name: "Franklin",
            occupation: "Chemist",
          }],
          [[0, 1, "Einstein", "Albert", 1], {
            id: 1,
            first_name: "Albert",
            last_name: "Einstein",
            occupation: "Physicist",
          }],
          [[0, 1, "Franklin", "Rosalind", 3], {
            id: 3,
            first_name: "Rosalind",
            last_name: "Franklin",
            occupation: "Chemist",
          }],
          [[0, 1, "Pascal", "Blaise", 2], {
            id: 2,
            first_name: "Blaise",
            last_name: "Pascal",
            occupation: "Mathematician",
          }],
          [[1, 0, 1], {
            id: 1,
            "name": "R&D",
          }],
          [[1, 0, 2], {
            id: 2,
            "name": "Mining",
          }]
        ]);
      });
    });
  });

  it("streams no events", function() {
    return this.replica.replicate(this.publisher);
  })

  it("writes index on commit", function() {
    this.publisher.events.push({
      type: "COMMIT",
      tx: "txname",
    });
    return this.replica.replicate(this.publisher).then(() => {
      expect(this.replica.indexName).to.equal("txname");
    });
  })

  it("streams row update", function() {
    this.publisher.events.push({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      row: { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })

  it("streams row update, ignoring extra column", function() {
    this.publisher.events.push({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      row: { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber", extra: "ignored" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })

  it("streams row insert", function() {
    this.publisher.events.push({
      type: "INSERT",
      schema: "public",
      name: "employee",
      row: { id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([0, 0, 4]);
    }).then(value => {
      expect(value).to.deep.equal({ id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" });
    });
  })

  it("streams row insert, ignoring extra column", function() {
    this.publisher.events.push({
      type: "INSERT",
      schema: "public",
      name: "employee",
      row: { id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist", extra: "ignored" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([0, 0, 4]);
    }).then(value => {
      expect(value).to.deep.equal({ id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" });
    });
  })

  it("streams row delete", function() {
    this.publisher.events.push({
      type: "DELETE",
      schema: "public",
      name: "employee",
      row: { id: 3 },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([0, 0, 3]);
    }).then(value => {
      expect(value).to.be.undefined;
    });
  })

  it("automatically does bulk operation after buffering enough rows", function() {
    for (let i = 0; i < this.config.bulkSize * 2 - 2; ++i) {
      this.publisher.events.push({
        type: "UPDATE",
        schema: "public",
        name: "project",
        row: { id: 2, name: i },
      });
    }
    return this.replica.replicate(this.publisher).then(() => {
      return this.tree.get([1, 0, 2]);
    }).then(value => {
      expect(value.name).to.equal(this.config.bulkSize - 1);
    });
  })

  it("ignores unknown event type", function() {
    this.publisher.events.push({
      type: "IGNORED",
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher);
  })

  it("ignores unknown schema", function() {
    this.publisher.events.push({
      type: "INSERT",
      schema: "unknown",
      name: "employee",
      row: { foo: "bar" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher);
  })

  it("ignores unknown table", function() {
    this.publisher.events.push({
      type: "INSERT",
      schema: "public",
      name: "unknown",
      row: { foo: "bar" },
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher);
  })
})
