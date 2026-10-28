# Arrays and maps in SQL

RustSpark enables DataFusion's nested-expression functions for SQL queries.
This is separate from `--json-array`, which reads a top-level JSON array
of records.

```sql
SELECT ARRAY_LENGTH(MAKE_ARRAY(10, 20, 30)) AS item_count;
SELECT ARRAY_HAS(MAKE_ARRAY(10, 20, 30), 20) AS contains_20;
SELECT MAP_KEYS(MAP(['region', 'tier'], ['eu', 'pro'])) AS keys;
SELECT MAP_VALUES(MAP(['region', 'tier'], ['eu', 'pro'])) AS values;
```

Nested arrays and maps use native Arrow types. Existing CSV, JSON and
Parquet table readers continue to use their original schema and execution
paths. This enables DataFusion SQL semantics, not full Spark compatibility.
In particular, `MAP_EXTRACT` returns a list of matching values, not a
Spark-style nullable scalar.

See [DataFusion's nested SQL functions](https://datafusion.apache.org/user-guide/sql/scalar_functions.html)
for supported functions and argument rules.
