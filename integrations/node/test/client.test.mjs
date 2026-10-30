import test from "node:test";
import assert from "node:assert/strict";
import { RustSpark, RustSparkError } from "../index.mjs";

test("passes typed parameters and file paths as argv without a shell", () => {
  let command;
  const client = new RustSpark({ binary: "/usr/local/bin/rustspark", runner: (bin, args, options) => {
    command = { bin, args, options };
    return { status: 0, stdout: '[{"sum":23}]', stderr: "" };
  } });
  const rows = client.query("SELECT SUM(n) AS sum FROM sales WHERE n > $min", {
    csvTables: { sales: "/data/my files.csv" }, namedParams: { min: "int64:3" }, maxRows: 2,
  });
  assert.deepEqual(rows, [{ sum: 23 }]);
  assert.equal(command.bin, "/usr/local/bin/rustspark");
  assert.equal(command.options.shell, false);
  assert.ok(command.args.includes("sales=/data/my files.csv"));
  assert.ok(command.args.includes("min=int64:3"));
  assert.equal(command.args[command.args.indexOf("--sql") + 1].includes("int64:3"), false);
});

test("rejects conflicting sources and malformed arguments", () => {
  const client = new RustSpark({ runner: () => { throw Error("should not run"); } });
  assert.throws(() => client.query("SELECT 1", { csv: "a.csv", parquet: "b.parquet" }), TypeError);
  assert.throws(() => client.query("SELECT 1", { csvTables: { "x=y": "a.csv" } }), TypeError);
  assert.throws(() => client.query("SELECT 1", { queryTimeout: 0 }), RangeError);
  assert.throws(() => client.query("SELECT 1", { maxRows: -2 }), RangeError);
});

test("reports process exit and invalid output", () => {
  assert.throws(() => new RustSpark({ runner: () => ({ status: 2, stderr: "Bad query" }) }).query("x"), /Bad query/);
  assert.throws(() => new RustSpark({ runner: () => ({ status: 0, stdout: "{}" }) }).query("x"), RustSparkError);
  assert.throws(() => new RustSpark({ runner: () => ({ error: Error("ENOENT") }) }).query("x"), /ENOENT/);
});


test("rejects non-finite numbers inside nested SQL JSON results", () => {
  for (const stdout of ['[{"n":1e999}]', '[{"nested":[-1e999]}]']) {
    const client = new RustSpark({ runner: () => ({ status: 0, stdout }) });
    assert.throws(() => client.query("SELECT 1"), RustSparkError);
  }
  const ok = new RustSpark({ runner: () => ({ status: 0, stdout: '[{"n":1.5}]' }) });
  assert.deepEqual(ok.query("SELECT 1"), [{ n: 1.5 }]);
});

test("queryOne caps cardinality and rejects multiple rows", () => {
  let command;
  const client = new RustSpark({ runner: (_bin, argv) => {
    command = argv;
    return { status: 0, stdout: '[{"id":1}]' };
  } });
  assert.deepEqual(client.queryOne("SELECT id FROM data"), { id: 1 });
  assert.equal(command[command.indexOf("--max-rows") + 1], "2");
  const empty = new RustSpark({ runner: () => ({ status: 0, stdout: "[]" }) });
  assert.equal(empty.queryOne("SELECT 1"), null);
  const duplicated = new RustSpark({ runner: () => ({ status: 0, stdout: "[{},{}]" }) });
  assert.throws(() => duplicated.queryOne("SELECT 1"), RustSparkError);
  assert.throws(() => client.queryOne("SELECT 1", { maxRows: 1 }), TypeError);
});

test("SQL file and deterministic table and parameter ordering", () => {
  let command;
  const client = new RustSpark({ runner: (_bin, argv) => {
    command = argv;
    return { status: 0, stdout: "[]" };
  } });
  client.query("", {
    sqlFile: "queries/report.sql",
    csvTables: { zeta: "/z.csv", alpha: "/a.csv" },
    namedParams: { zeta: "int64:2", alpha: "int64:1" },
  });
  assert.deepEqual(command.slice(0, 4), ["--sql-file", "queries/report.sql", "--stdout-format", "json-array"]);
  assert.ok(command.indexOf("alpha=/a.csv") < command.indexOf("zeta=/z.csv"));
  assert.ok(command.indexOf("alpha=int64:1") < command.indexOf("zeta=int64:2"));
  assert.throws(() => client.query("SELECT 1", { sqlFile: "x.sql" }), TypeError);
  assert.throws(() => client.query("", { sqlFile: "" }), TypeError);
});

test("rejects oversized or excessive named parameters before spawning", () => {
  const client = new RustSpark({ runner: () => { throw Error("should not run"); } });
  assert.throws(() => client.query("SELECT 1", { namedParams: { p: "utf8:" + "x".repeat(8192) } }), TypeError);
  assert.throws(() => client.query("SELECT 1", { namedParams: { p: "utf8:" + "é".repeat(4500) } }), TypeError);
  assert.throws(() => client.query("SELECT 1", {
    namedParams: Object.fromEntries(Array.from({ length: 65 }, (_, index) => ["p" + index, "int64:1"])),
  }), RangeError);
});

test("rejects empty named input file paths before invoking the CLI", () => {
  const client = new RustSpark({ runner: () => { throw new Error("should not run"); } });
  for (const option of ["csvTables", "parquetTables", "jsonTables"]) {
    assert.throws(() => client.query("SELECT 1", {
      [option]: { sales: "" },
    }), TypeError, option);
  }
  assert.throws(() => client.query("SELECT 1", {
    csvTables: { sales: null },
  }), TypeError);
});

test("rejects int64 SQL results that JSON.parse would round", () => {
  for (const stdout of [
    '[{"id":9007199254740993}]',
    '[{"id":-9007199254740993}]',
    '[{"id":9223372036854775807}]',
    '[{"nested":{"ids":[9007199254740992]}}]',
  ]) {
    const client = new RustSpark({ runner: () => ({ status: 0, stdout }) });
    assert.throws(() => client.query("SELECT id FROM t"), RustSparkError);
  }
  const valid = new RustSpark({ runner: () => ({
    status: 0, stdout: '[{"id":9007199254740991},{"id":-9007199254740991},{"float":1.25}]',
  }) });
  assert.deepEqual(valid.query("SELECT id FROM t"), [
    { id: 9007199254740991 }, { id: -9007199254740991 }, { float: 1.25 },
  ]);
});

test("named parameter length is measured in UTF-8 bytes", () => {
  let argv;
  const client = new RustSpark({ runner: (_binary, args) => {
    argv = args;
    return { status: 0, stdout: "[]" };
  } });
  const largest = "utf8:" + "é".repeat(4093);
  assert.equal(Buffer.byteLength(largest, "utf8"), 8191);
  assert.deepEqual(client.query("SELECT 1", { namedParams: { p: largest } }), []);
  assert.ok(argv.includes("p=" + largest));
});
