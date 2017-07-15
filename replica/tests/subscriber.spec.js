"use strict";

const { expect } = require("chai");
const { join } = require("path");
const sinon = require("sinon");

const { Subscriber } = require("../subscriber");
const { Tree } = require("../..");
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

describe("Subscriber", function() {
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
    this.publisher = new TestPublisher(this.tables);
    this.replica = new Subscriber(this.store, this.config);
  });

  it("loads existing rows of each index", function() {
    return this.replica.snapshot(this.publisher).then(() => {
      let result = [];
      return this.replica.tree.forEach((value, key) => {
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

  it("streams update", function() {
    this.publisher.events.push({
      type: "UPDATE",
      schema: "public",
      name: "employee",
      rows: [{ id: 2, first_name: "Blaise", last_name: "Pascal", occupation: "Plumber" }],
    });
    this.publisher.events.push({
      type: "COMMIT",
      tx: "1",
    });
    return this.replica.replicate(this.publisher).then(() => {
      return this.replica.tree.get([0, 0, 2]);
    }).then(value => {
      expect(value.occupation).to.equal("Plumber");
    });
  })
})
