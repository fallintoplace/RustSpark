import test from "node:test";
import assert from "node:assert/strict";
import { RustSpark, RustSparkError } from "../index.mjs";
import { parseStrictJSON } from "../strict-json.mjs";

test("rejects duplicate SQL result keys at nested depths", () => {
  for (const output of [
    '[{"id":1,"id":2}]',
    '[{"nested":{"id":1,"id":2}}]',
    '[{"records":[{"k":"a","k":"b"}]}]',
    '[{"a":1,"\\u0061":2}]',
  ]) {
    assert.throws(() => parseStrictJSON(output), /Duplicate/);
    assert.throws(
      () => new RustSpark({ runner: () => ({ status: 0, stdout: output }) }).query("SELECT 1"),
      RustSparkError,
    );
  }
});

test("keeps valid nested JSON output and numeric checks", () => {
  const output = '[{"key":{"a":[1,2,null],"b":"\\u00e9"},"n":123}]';
  assert.deepEqual(parseStrictJSON(output), [{ key: { a: [1, 2, null], b: "é" }, n: 123 }]);
  for (const invalid of [
    '[{"n":9007199254740993}]',
    '[{"n":1e999}]',
    '[{"x":1}] trailing',
    '[{"a":]',
    '[{"a":1,}]',
    '[] []',
  ]) {
    assert.throws(() => parseStrictJSON(invalid), invalid);
  }
});

test("bounds recursive JSON validation before exhausting the JavaScript stack", () => {
  const nested = "[".repeat(300) + "0" + "]".repeat(300);
  assert.throws(() => parseStrictJSON(nested), /nesting limit/);
});


test("validates large escaped text without decoding it during the pre-scan", () => {
  const text = "🙂\\\"é".repeat(100_000);
  const payload = JSON.stringify([{ name: text, nested: { text } }]);
  const originalParse = JSON.parse;
  let calls = 0;
  try {
    JSON.parse = (...args) => { calls++; return originalParse(...args); };
    const rows = parseStrictJSON(payload);
    assert.equal(rows[0].name, text);
    assert.equal(rows[0].nested.text, text);
    // Two object keys per object plus the final document parse, not a
    // second decode of either large text value.
    assert.equal(calls, 4);
  } finally {
    JSON.parse = originalParse;
  }
  assert.throws(() => parseStrictJSON(String.raw`[{"name":"broken\uZ"}]`), SyntaxError);
});

test("can retain unsafe JSON int64 results without rounding on supported Node", () => {
  const output = '[{"id":9223372036854775807,"nested":[-9223372036854775808,5]}]';
  const hasSource = JSON.parse("1", (_key, _value, ctx) => ctx?.source) === "1";
  if (!hasSource) {
    assert.throws(() => parseStrictJSON(output, { unsafeIntegers: "bigint" }), /source context/);
    return;
  }
  const expected = [{ id: 9223372036854775807n, nested: [-9223372036854775808n, 5] }];
  assert.deepEqual(parseStrictJSON(output, { unsafeIntegers: "bigint" }), expected);
  const client = new RustSpark({ runner: () => ({ status: 0, stdout: output }) });
  assert.deepEqual(client.query("SELECT id FROM data", { unsafeIntegers: "bigint" }), expected);
  assert.throws(() => parseStrictJSON('[{"n":1e20}]', { unsafeIntegers: "bigint" }), /Unsafe/);
  assert.throws(() => parseStrictJSON("[]", { unsafeIntegers: "string" }), /unsafeIntegers/);
});
