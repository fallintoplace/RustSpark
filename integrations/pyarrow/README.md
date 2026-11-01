# Stream Arrow batches into Python

RustSpark can stream Arrow IPC directly to stdout. The PyArrow reader
consumes batches incrementally through a pipe, preserving Arrow logical
types without converting results to JSON or Python row dictionaries.

```sh
python3 -m pip install pyarrow
cargo build --locked --release
```

```python
from integrations.pyarrow.stream import iter_arrow_batches

for batch in iter_arrow_batches(
    "SELECT city, amount FROM data",
    csv="tests/fixtures/sales.csv",
    binary="target/release/rustspark",
    query_timeout=30,
):
    print(batch.num_rows, batch.schema)
```

This streaming API keeps only currently consumed batches in application
memory. Do not call `list(...)` for billion-row results. Query execution
may spill to disk on the RustSpark side. Arrow IPC over a subprocess pipe
is **not** shared-memory zero-copy. This module is a standalone example,
not a remote client or server.

```sh
cd integrations/pyarrow && python3 -m unittest -v test_stream.py
```

For joins across multiple local inputs, provide named maps such as
`csv_tables={"users": "users.csv"}` and
`parquet_tables={"orders": "orders.parquet"}`. Use the corresponding
SQL table names in the query. Named inputs cannot be mixed with
unnamed `csv`, `parquet` or `json_input` paths.

Pass `database_dir="/path/to/db"` to stream registered tables without a file source. `max_rows` must be a nonnegative integer and `query_timeout` a positive integer. Child-process stderr is spooled to temporary disk storage while Arrow stdout streams, preventing a full diagnostic pipe from blocking the query. Only the trailing 8 KiB of diagnostics is read on failures.

## Process deadlines

Use `process_timeout=30` with `iter_arrow_batches` to enforce a wall-clock
deadline even when the native process stalls before completing Arrow IPC output.
The iterator terminates the process group on timeout and raises `TimeoutError`.
`query_timeout` still controls the native SQL deadline independently.

Typed SQL bindings are supported with `named_params={"min": "int64:7"}`. Parameters are passed as CLI arguments without a shell. Names must be ASCII SQL identifiers, values require a type prefix, and each value is limited to 8192 UTF-8 bytes (64 bindings maximum).
