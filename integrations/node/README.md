# RustSpark Node.js client

An ESM client for the local RustSpark executable. Node 20+ required.
No npm dependencies. It is intentionally **not** a Spark driver or a
multi-tenant SQL server.

```js
import { RustSpark } from "./integrations/node/index.mjs";

const client = new RustSpark({ binary: "./target/release/rustspark" });
const rows = client.query(
  "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
  { csv: "tests/fixtures/sales.csv", maxRows: 100, processTimeoutMs: 30000 },
);
console.log(rows);
```

For bound parameters, use `namedParams: { threshold: "int64:10" }`
in combination with `$threshold` in SQL. The client uses `spawnSync`
with `shell: false`, so file paths containing spaces remain safe.
Large results are held in memory. Use the raw RustSpark CLI with
`--stdout-format json` or `arrow` for streaming workloads.

```sh
cd integrations/node
npm test
```
