"use strict";

const { expect } = require("chai");
const sinon = require("sinon");

const { parseRecvLine, parseRecvLines } = require("../postgresql");

const has = Object.prototype.hasOwnProperty;

describe("Parse event", function() {
  it("parses begin transaction", function() {
    expect(parseRecvLine("BEGIN 123")).to.deep.equal({
      type: "BEGIN",
      tx: "123",
    });
  })

  it("parses commit transaction", function() {
    expect(parseRecvLine("COMMIT 123")).to.deep.equal({
      type: "COMMIT",
      tx: "123",
    });
  })

  it("parses commit transaction with timestamp", function() {
    expect(parseRecvLine("COMMIT 123 (at 2000-01-01 etc)")).to.deep.equal({
      type: "COMMIT",
      tx: "123",
    });
  })

  it("parses UPDATE", function() {
    expect(parseRecvLine("table public.foo: UPDATE:")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: {},
    });
  })

  it("parses INSERT", function() {
    expect(parseRecvLine("table public.foo: INSERT:")).to.deep.equal({
      type: "INSERT",
      schema: "public",
      name: "foo",
      row: {},
    });
  })

  it("parses DELETE", function() {
    expect(parseRecvLine("table public.foo: DELETE:")).to.deep.equal({
      type: "DELETE",
      schema: "public",
      name: "foo",
      row: {},
    });
  })

  it("parses null column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[text]:null")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: null },
    });
  })

  it("parses boolean column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[boolean]:true c2[boolean]:false")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: true, c2: false },
    });
  })

  it("parses number column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[real]:-7.25")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: -7.25 }
    });
  })

  it("parses integer column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[integer]:-7")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: -7 },
    });
  })

  it("parses text column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[text]:'\\Hello, ''Al''\n'")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: "\\Hello, 'Al'\n" },
    });
  })

  it("parses json column", function() {
    expect(parseRecvLine("table public.foo: UPDATE: c1[jsonb]:'[1, 2]' c2[json]:'null'")).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { c1: [1, 2], c2: null },
    });
  })

  it("parses quoted schema", function() {
    expect(parseRecvLine(`table "quoted.schema".foo: UPDATE:`)).to.deep.equal({
      type: "UPDATE",
      schema: "quoted.schema",
      name: "foo",
      row: {},
    });
  })

  it("parses quoted table name", function() {
    expect(parseRecvLine(`table public."quoted.table": UPDATE:`)).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "quoted.table",
      row: {},
    });
  })

  it("parses quoted column name", function() {
    expect(parseRecvLine(`table public.foo: UPDATE: "myCol"[text]:'Hello'`)).to.deep.equal({
      type: "UPDATE",
      schema: "public",
      name: "foo",
      row: { myCol: "Hello" },
    });
  })
})

describe("Parse lines", function() {
  it("parses single line", function() {
    let handleEvent = sinon.stub();
    return parseRecvLines(`table public.foo: UPDATE: col[text]:2\n`, handleEvent).then(rest => {
      expect(rest).to.equal("");
      sinon.assert.calledWith(handleEvent, {
        type: "UPDATE",
        schema: "public",
        name: "foo",
        row: { col: "2" },
      });
    });
  })

  it("parses two lines", function() {
    let handleEvent = sinon.stub();
    return parseRecvLines(`table public.foo: UPDATE: col[text]:2\ntable public.foo: UPDATE: col[text]:3\n`, handleEvent).then(rest => {
      expect(rest).to.equal("");
      sinon.assert.calledTwice(handleEvent);
    });
  })

  it("ignores partial line", function() {
    let handleEvent = sinon.stub();
    return parseRecvLines(`table public.foo: UPDATE: col[text]:2\ntable public.foo:`, handleEvent).then(rest => {
      expect(rest).to.equal("table public.foo:");
      sinon.assert.calledOnce(handleEvent);
    });
  })

  it("newlines in single quotes do not terminate line", function() {
    let handleEvent = sinon.stub();
    return parseRecvLines(`table public.foo: UPDATE: col[text]:'foo\n'\n`, handleEvent).then(rest => {
      expect(rest).to.equal("");
      sinon.assert.calledWith(handleEvent, {
        type: "UPDATE",
        schema: "public",
        name: "foo",
        row: { col: "foo\n" },
      });
    });
  })

  it("newlines in double quotes do not terminate line", function() {
    let handleEvent = sinon.stub();
    return parseRecvLines(`table public.foo: UPDATE: "c\nol"[text]:3\n`, handleEvent).then(rest => {
      expect(rest).to.equal("");
      sinon.assert.calledWith(handleEvent, {
        type: "UPDATE",
        schema: "public",
        name: "foo",
        row: { "c\nol": "3" },
      });
    });
  })
})
