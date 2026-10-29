# Pandas DataFrame integration

Requires `pandas`, `pyarrow`, and a RustSpark executable.
Data is exchanged via a temporary Arrow IPC file, not JSON, retaining
column types where pandas and Arrow support them.

```python
from integrations.pandas.query_dataframe import query_dataframe

frame = query_dataframe(
    "SELECT city, SUM(amount) AS total FROM data GROUP BY city",
    csv="tests/fixtures/sales.csv",
    binary="target/release/rustspark",
    max_rows=1000,
)
print(frame)
```

This helper defaults to **100,000 result rows** to reduce accidental
out-of-memory crashes. `max_rows` limits returned rows, not input
scan size. Pandas still materializes the result in memory and can use
significantly more memory than its Arrow representation. For billion-row
results use RustSpark's Arrow stream instead.

```sh
cd integrations/pandas && python3 -m unittest -v test_query_dataframe.py
```

To query a persistent RustSpark database rather than supplying a file,
pass `database_dir="/path/to/rustspark-database"`.

Use `json_input="events.json"` for JSON sources, or `csv_tables={"users": "users.csv"}` and `parquet_tables={"orders": "orders.parquet"}` to query named tables in joins. Named and unnamed sources cannot be mixed. Use `named_params={"minimum": "int64:10"}` for typed SQL parameters. `process_timeout=30` limits the full subprocess (including export), separately from `query_timeout`.
