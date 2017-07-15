"use strict";

const { Replica } = require ("./replica");
const { Tree } = require("..");

const has = Object.prototype.hasOwnProperty;

class Publisher {
  constructor(store, config) {
    let tree = Tree.empty(store);
    this.replica = new Replica(tree, config);
  }

  copyTables() {
    let config = this.replica.config;

    let tableIdx = -1;
    const copyTables = () => {
      ++tableIdx;
      let config = this.replica.config;

      if (tableIdx >= config.tables.length)
        return Promise.resolve();
      
      let table = config.tables[tableIdx];
      let primaryKeyPath = table.indices[0].keyPath;
      let cursor = this.query(table, primaryKeyPath);

      const copyRows = () => {
        return cursor.read().then(rows => {
          if (!rows.length)
            return;

          return this.replica.onEvent({
            type: "INSERT",
            schema: table.schema,
            name: table.name,
            rows: rows,
          }).then(copyRows);
        })
      };

      return copyRows().then(copyTables);
    }

    return copyTables().then(() => {
      return this.replica.onEvent({
        type: "COMMIT",
        tx: "initial",
      });
    });
  }

  replicate() {
    return this.snapshot().then(() => {
      return this.stream();
    });
  }
}

module.exports = {
  Publisher,
}
