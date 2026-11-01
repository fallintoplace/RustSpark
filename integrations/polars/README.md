# Polars lazy scan integration

Requires optional `polars` and a built RustSpark CLI. Export the SQL
result once as an Arrow IPC **file**, then scan lazily from Polars.

```python
import polars as pl
from integrations.polars.query_lazy import rustspark_lazy_frame

with rustspark_lazy_frame(
    "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
    csv="tests/fixtures/sales.csv",
    binary="target/release/rustspark",
) as frame:
    print(frame.filter(pl.col("total") > 10).collect())
```

**Keep the LazyFrame inside the `with` block.** Its backing temporary
file is removed when the block exits. RustSpark must finish exporting
before the Polars lazy scan starts. This is not distributed query pushdown;
Polars applies subsequent expressions to the Arrow IPC result. Choose an
adequate temp volume for large intermediate results.

```sh
cd integrations/polars && python3 -m unittest -v test_query_lazy.py
```

Use `database_dir="/path/to/rustspark-database"` to export queries
against registered tables in persistent RustSpark databases.

Use `json_input="events.json"` for JSON input or `csv_tables={"users": "users.csv"}` and `parquet_tables={"orders": "orders.parquet"}` for named-table joins. Named and unnamed sources cannot be mixed. Set `max_rows=10000` to cap exported rows and `process_timeout=30` to bound the full subprocess, in addition to `query_timeout`.

The context manager now terminates and reaps the entire Arrow export process
group when an export deadline expires or a wait is interrupted. This avoids
orphaned native workers when a lazy-frame export fails.

Typed SQL bindings are supported with `named_params={"min": "int64:7"}`. Parameters are passed as CLI arguments without a shell. Names must be ASCII SQL identifiers, values require a type prefix, and each value is limited to 8192 UTF-8 bytes (64 bindings maximum).
