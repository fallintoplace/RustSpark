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
