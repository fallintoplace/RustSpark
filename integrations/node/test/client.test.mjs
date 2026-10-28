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
