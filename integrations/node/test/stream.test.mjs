import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RustSpark, RustSparkError } from "../index.mjs";

const shellTest = process.platform === "win32" ? test.skip : test;

async function withScript(script, run) {
  const directory = mkdtempSync(join(tmpdir(), "rustspark-node-test-"));
  const binary = join(directory, "fake-rustspark");
  writeFileSync(binary, "#!/bin/sh\n" + script, { mode: 0o700 });
  try {
    return await run(new RustSpark({ binary }));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

shellTest("queryRows preserves row precision and streams multiple JSON lines", async () => {
  await withScript("printf '%s\\n' '{\"id\":9223372036854775807}' '{\"id\":2}'\n", async (client) => {
    const rows = [];
    for await (const row of client.queryRows("SELECT 1")) rows.push(row);
    // The default parser rejects unsafe integers, never rounding them.
    assert.fail("unsafe int64 result must fail");
  }).catch((error) => assert.ok(error instanceof RustSparkError));
  await withScript("printf '%s\\n' '{\"id\":1}' '{\"id\":2}'\n", async (client) => {
    const rows = [];
    for await (const row of client.queryRows("SELECT 1")) rows.push(row);
    assert.deepEqual(rows, [{ id: 1 }, { id: 2 }]);
  });
});

shellTest("queryRows rejects duplicate nested keys and oversized output", async () => {
  await withScript("printf '%s\\n' '{\"nested\":{\"x\":1,\"x\":2}}'\n", async (client) => {
    await assert.rejects(async () => {
      for await (const _row of client.queryRows("SELECT 1")) {}
    }, RustSparkError);
  });
  await withScript("printf '%s\\n' '{\"a\":111111}' '{\"a\":2}'\n", async (client) => {
    await assert.rejects(async () => {
      for await (const _row of client.queryRows("SELECT 1", { maxOutputBytes: 12 })) {}
    }, /maxOutputBytes/);
  });
});

shellTest("breaking async iteration closes the exporter instead of hanging", async () => {
  await withScript("printf '%s\\n' '{\"n\":1}'\nwhile :; do printf '%s\\n' '{\"n\":2}'; done\n", async (client) => {
    let count = 0;
    for await (const row of client.queryRows("SELECT 1", { processTimeoutMs: 2000 })) {
      assert.deepEqual(row, { n: 1 });
      count++;
      break;
    }
    assert.equal(count, 1);
  });
});
