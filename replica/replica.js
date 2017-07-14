"use strict";

const has = Object.prototype.hasOwnProperty;

class Replica {
  constructor(tree, client, config) {
    config = Object.assign({
      bulkSize: 1000,
    }, config);

    this.client = client;
    this.config = config;
    this.tree = tree;
  }

  snapshot() {
    let tableIdx = -1;
    const copyTables = () => {
      ++tableIdx;
      if (tableIdx >= this.config.tables.length)
        return;
      
      let table = this.config.tables[tableIdx];
      let primaryKeyPath = table.indices[0].keyPath;
      let cursor = this.client.query(table, primaryKeyPath);

      const copyRows = () => {
        return cursor.read().then(rows => {
          if (!rows.length)
            return;

          let indexIdx = -1;
          const copyIndices = () => {
            ++indexIdx;
            if (indexIdx >= table.indices.length)
              return;

            let index = table.indices[indexIdx];
            let keyPath = index.keyPath;
            if (indexIdx > 0 && !index.unique)
              keyPath = keyPath.concat(primaryKeyPath);

            let bulkRows = [];
            let keyPrefix = [tableIdx, indexIdx];
            rows.forEach(row => {
              let key = keyPrefix.concat(keyPath.map(c => row[c]));
              let value = row.map;
              bulkRows.push([key, row]);
            });

            return this.tree.bulk(bulkRows).then(copyIndices);
          };

          return copyIndices().then(copyRows);
        })
      };

      return copyRows().then(copyTables);
    }

    return this.client.snapshot(copyTables).then(() => {
      return this.tree.commit("initial");
    });
  }

  stream() {
    let bulkRows = [];
    let count = 0;
    let writing = Promise.resolve();

    let byName = {};
    this.config.tables.forEach((table, tableIdx) => {
      if (!has.call(byName, table.schema))
        byName[table.schema] = {};
      byName[table.schema][table.name] = tableIdx;
    });

    const writeRows = () => {
      if (bulkRows.length === 0)
        return;

      let rows = bulkRows;
      count += rows.length;
      bulkRows = [];
      writing = writing.then(() => {
        return this.tree.bulk(rows)
      });
    }

    const commit = (name) => {
      writeRows();
      writing = writing.then(() => {
        return this.tree.commit(name)
      }).then(() => {
        count = 0;
      });
    }

    const handleEvent = (event) => {
      if (event.type === "BEGIN") {
        return;
      } else if (event.type === "COMMIT") {
        commit(event.tx);
        return;
      } else if (event.type === "UPDATE" || event.type === "INSERT" || event.type === "DELETE") {
        if (!has.call(byName, event.schema))
          return;
        if (!has.call(byName[event.schema], event.name))
          return;
        let tableIdx = byName[event.schema][event.name];
        let table = this.config.tables[tableIdx];
        let primaryKeyPath = table.indices[0].keyPath;

        table.indices.forEach((index, indexIdx) => {
          let keyPath = index.keyPath;
          if (indexIdx > 0 && !index.unique)
            keyPath = keyPath.concat(primaryKeyPath);

          let keyPrefix = [tableIdx, indexIdx];
          let key = keyPrefix.concat(keyPath.map(c => event.row[c]));
          let value = event.row;

          if (event.type === "DELETE") {
            bulkRows.push([key]);
          } else {
            bulkRows.push([key, value]);
          }
        });
      } else {
        return;
      }

      if (bulkRows.length >= this.config.bulkSize)
        writeRows();
    }

    return this.client.stream(handleEvent).then(() => {
      return writing;
    });
  }

  replicate() {
    return this.snapshot().then(() => {
      return this.stream();
    });
  }
}

module.exports = {
  Replica,
}