"use strict"; 

const has = Object.prototype.hasOwnProperty;

class Replica {
  constructor(tree, config) {
    config = Object.assign({
      bulkSize: 1000,
    }, config);

    this.config = config;
    this.committed = tree;
    this.uncommitted = tree.clone();
    this.indexName = undefined;

    let byName = {};
    this.streaming = {
      byName: byName,
      rows: [],
      count: 0,
      writing: Promise.resolve(),
    };
    config.tables.forEach((table, tableIdx) => {
      if (!has.call(byName, table.schema))
        byName[table.schema] = {};
      byName[table.schema][table.name] = tableIdx;
    });
  }

  get tree() {
    return this.committed;
  }

  onEvent(event) {
    if (event.type === "COMMIT") {
      return this.commit(event.tx);
    } else if (event.type === "UPDATE" || event.type === "INSERT" || event.type === "DELETE") {
      let byName = this.streaming.byName;
      if (has.call(byName, event.schema) && has.call(byName[event.schema], event.name)) {
        let tableIdx = byName[event.schema][event.name];
        let table = this.config.tables[tableIdx];
        let primaryKeyPath = table.indices[0].keyPath;

        table.indices.forEach((index, indexIdx) => {
          let keyPath = index.keyPath;
          if (indexIdx > 0 && !index.unique)
            keyPath = keyPath.concat(primaryKeyPath);

          let keyPrefix = [tableIdx, indexIdx];
          let key = keyPrefix.concat(keyPath.map(c => event.row[c]));

          let value;
          if (table.columns === undefined) {
            value = event.row;
          } else {
            value = {};
            for (let cn in table.columns) {
              if (has.call(table.columns, cn)) {
                value[cn] = event.row[cn];
              }
            }
          }

          if (event.type === "DELETE") {
            this.streaming.rows.push([key]);
          } else {
            this.streaming.rows.push([key, value]);
          }
        });
      }
    }

    if (this.streaming.rows.length >= this.config.bulkSize)
      return this.flush();

    return Promise.resolve();
  }

  flush() {
    let rows = this.streaming.rows;
    if (rows.length === 0)
      return this.streaming.writing;

    this.streaming.count += rows.length;
    this.streaming.rows = [];
    return this.streaming.writing = this.streaming.writing.then(() => {
      return this.uncommitted.bulk(rows)
    });
  }

  commit(name) {
    this.flush();
    return this.streaming.writing = this.streaming.writing.then(() => {
      return this.uncommitted.commit(name)
    }).then(() => {
      this.committed = this.uncommitted;
      this.uncommitted = this.uncommitted.clone();
      this.indexName = name;
      this.streaming.count = 0;
    });
  }
}

module.exports = {
  Replica,
}
