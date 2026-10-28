# RustSpark Python client (local subprocess)

This lightweight client executes the existing RustSpark CLI. It does not
create a database server or claim Spark API compatibility.

```sh
python3 -m pip install ./integrations/python
cargo build --release --locked
export PATH="$PWD/target/release:$PATH"
```

```python
from rustspark_local import RustSpark

client = RustSpark()
rows = client.query(
    "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
    csv="tests/fixtures/sales.csv",
    max_rows=1000,
    process_timeout=30,
)
print(rows)
```

For bound parameters, use `named_params={"minimum": "int64:10"}`
and reference `$minimum` in SQL. All inputs are passed as independent
process arguments, not through a shell or SQL string substitution.
The `process_timeout` starts before file registration; the optional
`query_timeout` is handled separately by the RustSpark CLI.

`query()` loads the full result JSON into memory. Set `max_rows` or
choose the raw CLI's Arrow or NDJSON streaming for large results. The
local database is single-writer and is not suitable for multi-tenant
untrusted SQL execution.

Test without a compiled RustSpark executable:

```sh
PYTHONPATH=integrations/python/src python3 -m unittest discover -s integrations/python/tests
```
