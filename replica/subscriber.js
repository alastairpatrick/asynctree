"use strict";

const { Replica } = require ("./replica");
const { Tree } = require("..");

const has = Object.prototype.hasOwnProperty;

class Subscriber extends Replica {
  constructor(store, config) {
    let tree = Tree.empty(store);
    super(tree, config);
  }

  snapshot(publisher) {
    let tableIdx = -1;
    const copyTables = () => {
      ++tableIdx;
      if (tableIdx >= this.config.tables.length)
        return;
      
      let table = this.config.tables[tableIdx];
      let primaryKeyPath = table.indices[0].keyPath;
      let cursor = publisher.query(table, primaryKeyPath);

      const copyRows = () => {
        return cursor.read().then(rows => {
          if (!rows.length)
            return;

          return this.onEvent({
            type: "INSERT",
            schema: table.schema,
            name: table.name,
            rows: rows,
          }).then(copyRows);
        })
      };

      return copyRows().then(copyTables);
    }

    return publisher.snapshot(copyTables).then(() => {
      return this.onEvent({
        type: "COMMIT",
        tx: "initial",
      });
    });
  }

  stream(publisher) {
    return publisher.stream(this.onEvent.bind(this)).then(() => {
      return this.flush();
    });
  }

  replicate(publisher) {
    return this.snapshot(publisher).then(() => {
      return this.stream(publisher);
    });
  }
}

module.exports = {
  Subscriber,
}
