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

class TestClient {
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
    this.client = new TestClient(this.tables);
    this.replica = new Replica(this.tree, this.client, this.config);
  });

  it("loads existing rows of each index", function() {
    return this.replica.snapshot().then(() => {
      let result = [];
      return this.tree.forEach((value, key) => {
        result.push([key, value]);
      }).then(() => {
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
    return this.replica.replicate();
  })

  it("streams row update", function() {
    this.client.events.push({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      row: { id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber" },
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate().then(() => {
      return this.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })

  it("streams row insert", function() {
    this.client.events.push({
      type: "INSERT",
      schema: "public",
      name: "employee",
      row: { id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" },
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate().then(() => {
      return this.tree.get([0, 0, 4]);
    }).then(value => {
      expect(value).to.deep.equal({ id: 4, first_name: "Marie", last_name: "Curie", occupation: "Physicist" });
    });
  })

  it("streams row delete", function() {
    this.client.events.push({
      type: "DELETE",
      schema: "public",
      name: "employee",
      row: { id: 3 },
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate().then(() => {
      return this.tree.get([0, 0, 3]);
    }).then(value => {
      expect(value).to.be.undefined;
    });
  })

  it("automatically does bulk operation after buffering enough rows", function() {
    for (let i = 0; i < this.config.bulkSize * 2 - 2; ++i) {
      this.client.events.push({
        type: "UPDATE",
        schema: "public",
        name: "project",
        row: { id: 2, count: i },
      });
    }
    return this.replica.replicate().then(() => {
      return this.tree.get([1, 0, 2]);
    }).then(value => {
      expect(value.count).to.equal(this.config.bulkSize - 1);
    });
  })

  it("ignores unknown event type", function() {
    this.client.events.push({
      type: "IGNORED",
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate();
  })

  it("ignores unknown schema", function() {
    this.client.events.push({
      type: "INSERT",
      schema: "unknown",
      name: "employee",
      row: { foo: "bar" },
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate();
  })

  it("ignores unknown table", function() {
    this.client.events.push({
      type: "INSERT",
      schema: "public",
      name: "unknown",
      row: { foo: "bar" },
    });
    this.client.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate();
  })
})
