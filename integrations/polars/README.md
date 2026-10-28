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
