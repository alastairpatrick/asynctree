"use strict";

const { spawn } = require("child_process");
const pg = require("pg");
const PGCursor = require("pg-cursor");
const { Writable } = require("stream");
const { StringDecoder } = require("string_decoder");
const XRegExp = require('xregexp');

const { Publisher } = require("./publisher");

const has = Object.prototype.hasOwnProperty;

class Cursor {
  constructor(cursor) {
    this.cursor = cursor;
    this.pending = this.readAhead_(100);
  }

  read() {
    let pending = this.pending;
    this.pending = pending.then(() => this.readAhead_(1000));
    return pending;
  }

  readAhead_(num) {
    return new Promise((resolve, reject) => {
      this.cursor.read(num, (error, rows) => {
        if (error)
          reject(error);
        else
          resolve(rows);
      });
    });
  }
}

const BEGIN_RE = XRegExp(`^BEGIN (?<tx>[0-9]+)`, "sn");
const COMMIT_RE = XRegExp(`^COMMIT (?<tx>[0-9]+)`, "sn");
const DOUBLE_QUOTE_RE = XRegExp(`''`, "sg");
const ARRAY_TYPE_RE = XRegExp(`.*\\[\\]$`, "s");

const LINE_RE = XRegExp(`
  # One line
  (?<line>
    (
        [^'"\\n]*    # Unquoted charactrts
      | (' [^']* ')   # Single quoted characters
      | (" [^"]* ")   # Double quoted characters
    )*
  \\n)

  # Rest of buffer
  (?<rest>
    .*
  )`,
  "snx");

const OPERATION_RE = XRegExp(`
  ^table\\ 
  (?<schema>
    ( [^"][^\.]* ) | ( ("[^"]*")+ )
  )\.(?<table>
    ( [^"][^:]* ) | ( ("[^"]*")+ )
  ):\\ (?<operation>
    [A-Z]+
  ):(?<columns>
    .*
  )$`,
  "snx");

const COLUMN_RE = XRegExp(`
  \\s*(?<name>
    ( [^\\[]+ ) | ( ("[^"]*")+ )
  )\\[(?<type>
    [^:]+
  )\\]:(?<value>
    (
      [^'][^\\ ]*
    )|(
      '[^']*'
    )*
  )`,
  "sxg");

const typeOids = {
  "boolean": 16,
  "bigint": 20,
  "smallint": 21,
  "integer": 23,
  "text": 25,
  "oid": 26,
  "real": 700,
  "double precision": 701,
  "date": 1082,
  "timestamp without time zone": 1114,
  "timestamp with time zone": 1184,
  "point": 600,
  "circle": 718,
  "boolean[]": 1000,
  "bytea[]": 1001,
  "smallint[]": 1005,
  "integer[]": 1007,
  "text[]": 1009,
  "oid[]": 1028,
  "bigint[]": 1016,
  "point[]": 1017,
  "real[]": 1021,
  "double precision[]": 1022,
  "numeric[]": 1231,
  "timestamp without time zone[]": 1115,
  "date[]": 1182,
  "timestamp with time zone[]": 1185,
  "interval": 1186,
  "bytea": 17,
  "json": 114,
  "jsonb": 3802,
  "json[]": 199,
  "jsonb[]": 3807,
};

const typeParsers = {};

for (let n in typeOids) {
  if (has.call(typeOids, n)) {
    typeParsers[n] = pg.types.getTypeParser(typeOids[n], "text");
  }
}

const unquote = (s) => {
  if (s[0] === '"' && s[s.length - 1] === '"')
    return s.substring(1, s.length - 1);
  else
    return s;
}

const parseRecvLines = (unparsed, handleEvent) => {
  let match = XRegExp.exec(unparsed, LINE_RE);
  if (!match)
    return Promise.resolve(unparsed);

  return Promise.resolve(handleEvent(parseRecvLine(match.line))).then(() => {
    return parseRecvLines(match.rest, handleEvent);
  });
}

const parseRecvLine = (line) => {
  line = line.trim();

  let match;
  if (match = XRegExp.exec(line, BEGIN_RE)) {
    return {
      type: "BEGIN",
      tx: match.tx,
    };
  } else if (match = XRegExp.exec(line, COMMIT_RE)) {
    return {
      type: "COMMIT",
      tx: match.tx,
    };
  } else if (match = XRegExp.exec(line, OPERATION_RE)) {
    let row = {};
    XRegExp.forEach(match.columns, COLUMN_RE, colMatch => {
      let value = colMatch.value;
      if (value === "null") {
        value = null;
      } else {
        if (value[0] === "'" && value[value.length - 1] === "'")
          value = value.substring(1, value.length - 1).replace(DOUBLE_QUOTE_RE, "'");

        let type = colMatch.type;
        let parser;
        if (has.call(typeParsers, type)) {
          parser = typeParsers[type];
        } else {
          if (ARRAY_TYPE_RE.test(type))
            parser = typeParsers["text[]"];
          else
            parser = typeParsers["text"];
        }

        value = parser(value);
        if (value && typeof value.toJSON === "function")
          value = value.toJSON();
      }

      row[unquote(colMatch.name)] = value;
    });

    return {
      type: match.operation,
      schema: unquote(match.schema),
      name: unquote(match.table),
      rows: [row],
    };
  } else {
    return {
      type: "UNKNOWN",
      line: line,
    };
  }
}

class PGPublisher extends Publisher {
  constructor(store, config, client, slotName = "replica") {
    super(store, config);
    this.client = client;
    this.slotName = slotName;
    this.createdSlot = false;
  }

  end() {
    let promise = Promise.resolve();

    if (this.createdSlot) {
      this.createdSlot = false;
      promise = promise.then(() => this.client.query(`SELECT pg_drop_replication_slot($1)`, [this.slotName]));
      promise = promise.then(() => this.client.query(`COMMIT`));
    }

    return promise = promise.then(() => new Promise(resolve => {
      this.client.end(resolve);
    }));
  }

  snapshot() {
    const createSlot = (drop) => {
      let promise = this.client.query(`
        BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE
                          READ ONLY
                          DEFERRABLE
                          ;`);

      if (drop) {
        promise = promise.then(() => {
          return this.client.query(`SELECT pg_drop_replication_slot($1)`, [this.slotName]);
        });
      }

      promise = promise.then(() => {
        return this.client.query(`SELECT pg_create_logical_replication_slot($1, 'test_decoding')`, [this.slotName]);
      });

      return promise;
    }

    return Promise.resolve().then(() => {
      return createSlot(false).catch(error => {
        if (error.code != 53400 && error.code != 42710)
          throw error;
        return this.client.query(`ROLLBACK`).then(() => createSlot(true));
      });
    }).then(() => {
      return this.client.query("SHOW SERVER_ENCODING");
    }).then(result => {
      this.encoding = result.rows[0].server_encoding;
      return this.copyTables();
    }).then(result => {
      return this.client.query(`COMMIT`).then(() => {
        this.createdSlot = true;
        return result;
      });
    }).catch(error => {
      return this.client.query(`ROLLBACK`).then(() => {
        throw error;
      });
    });
  }

  query(table, orderByKeyPath) {
    let sql = `
        SELECT *
          FROM "${table.schema}"."${table.name}"
      ORDER BY ${orderByKeyPath.map(c => `"${c}"`).join(", ")}
             ;`;
    return new Cursor(this.client.query(new PGCursor(sql)));
   }

  stream() {
    return new Promise((resolve, reject) => {
      let connect = this.client;
      let options = {
        env: Object.assign({}, process.env, {
          PGHOST: connect.host,
          PGPORT: connect.port,
          PGDATABASE: connect.database,
          PGUSER: connect.user,
          PGPASSWORD: connect.password,
        }),
        stdio: ["pipe", "pipe", process.stderr],
      };

      if (!connect.password)
        delete options.env.PGPASSWORD;

      let child = spawn("pg_recvlogical", [`--start`, `--slot=${this.slotName}`, `--file=-`, `--dbname=${connect.database}`, `--option=skip-empty-xacts=1`], options);

      let unparsed = "";
      let decoder = new StringDecoder(this.encoding);

      let writable = new Writable();
      writable._write = (chunk, enc, next) => {
        if (typeof chunk === "string")
          unparsed += chunk;
        else
          unparsed += decoder.write(chunk);

        return parseRecvLines(unparsed, this.replica.onEvent.bind(this.replica)).then(rest => {
          unparsed = rest;
          next();
        }).catch(error => {
          next(error);
        });
      };

      child.stdout.pipe(writable);

      child.on("close", code => {
        if (code === 0)
          resolve();
        else
          reject(new Error(`pg_recvlogical exited with code ${code}.`));
      });
    });
  }
}

module.exports = {
  PGPublisher,
  parseRecvLine,
  parseRecvLines,
}
