# RustSpark

Experimental local SQL over CSV and Parquet files using Apache DataFusion. Not Spark-compatible.

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales --sql 'SELECT city, SUM(amount) FROM sales GROUP BY city'
```

NDJSON files can also be queried directly:

```sh
cargo run -- --json tests/fixtures/sales.json --sql 'SELECT COUNT(*) FROM data'
```

Provide a typed JSON schema when types must remain stable across files:

```sh
cargo run -- --json tests/fixtures/sales.json --json-schema 'city:utf8,amount:int64' --sql 'SELECT SUM(amount) FROM data'
```

The schema is shared by named JSON inputs too.

SQL statements can be read from a file:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql-file tests/fixtures/count_sales.sql
```

Run multiple SQL statements from `--sql` or `--sql-file`:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'CREATE VIEW large AS SELECT * FROM data WHERE amount >= 10; SELECT COUNT(*) FROM large;'
```

Statements run in order in the same session. Earlier statements execute without
printing their results; only the final statement uses the selected output mode.
Semicolons inside SQL string literals and comments are handled by DataFusion's
SQL parser. The query deadline covers all script statements.

Treat a literal CSV token as NULL, including tokens with regex punctuation:

```sh
cargo run -- --csv tests/fixtures/sales.csv --csv-null-value NA --sql 'SELECT COUNT(city) FROM data'
```

`--csv-null-value` matches the entire field literally (not as a regular expression).
It normalizes missing values before casting to inferred column types. This mode
may read more data than the regular CSV path, so use it only when needed.

Use a custom CSV delimiter with `--csv-delimiter`:

```sh
cargo run -- --csv tests/fixtures/sales_semicolon.csv --csv-delimiter ';' --sql 'SELECT SUM(amount) FROM data'
```

Join multiple CSV files in one query by naming each input:

```sh
cargo run -- --csv-table left=tests/fixtures/sales.csv --csv-table right=tests/fixtures/sales.csv --sql 'SELECT COUNT(*) FROM left JOIN right USING (city)'
```

Enable Parquet late materialization for selective filters:

```sh
cargo run -- --parquet /path/to/events.parquet \
  --parquet-pushdown-filters --parquet-reorder-filters \
  --sql "SELECT COUNT(*) FROM data WHERE event_type = 'purchase'"
```

These options push predicates into Parquet decoding and can reorder them by
estimated selectivity. They are opt-in because the benefit depends on file
layout and filter cost. Row-group pruning and page-index reads remain on by
default in DataFusion.

Query Hive-style directory partitions as typed SQL columns:

```sh
# For files such as /data/events/year=2025/month=10/part-0.parquet
cargo run -- --parquet /data/events \\
  --parquet-partitions year:int32,month:int32 \\
  --sql 'SELECT month, COUNT(*) FROM data WHERE year = 2025 GROUP BY month'
```

Partition columns are supplied by directory names, not read from Parquet
file contents. DataFusion can prune directories for predicates on those
columns. The flag also applies to all `--parquet-table` inputs.

Create lazy SQL views for reusable queries. Views can refer to earlier views:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql-view 'large=SELECT * FROM data WHERE amount >= 10' \\
  --sql 'SELECT SUM(amount) FROM large'
```

Bound the number of result rows printed with `--max-rows`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data' --max-rows 10
```

Cache a table in memory before running SQL (use only when it fits in RAM):

```sh
cargo run -- --csv tests/fixtures/sales.csv --cache-table data --sql 'SELECT SUM(amount) FROM data'
```

The `--cache-table` flag can be repeated for distinct tables.

Print only the number of rows returned by a query:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data WHERE amount >= 10' --count-only
```

This counts the query result rows, not the rows in its source tables.

Standalone SQL without a file also works:

```sh
cargo run -- --sql 'SELECT 42 AS answer'
```

Bind typed positional SQL parameters instead of interpolating user values into SQL:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'SELECT COUNT(*) FROM data WHERE amount > $1 AND city = $2' \
  --param int64:10 --param utf8:Berlin
```

Repeat `--param` in `$1`, `$2`, ... order. Supported types are
`int64`, `float64`, `bool`, `utf8`, and `null:`.
String values may contain colons. Parameters use DataFusion binding, not
SQL text substitution.

Use named parameters for queries where argument order may change:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql 'SELECT COUNT(*) FROM data WHERE amount > $minimum AND city = $city' \\
  --named-param minimum=int64:10 --named-param city=utf8:Berlin
```

Names must be SQL identifiers. Use either `--named-param` or `--param`,
not both. Values are bound safely without rewriting SQL text.

Provide column types to avoid CSV schema inference:

```sh
cargo run -- --csv tests/fixtures/sales.csv --csv-schema 'city:utf8,amount:int64' --sql 'SELECT SUM(amount) FROM data'
```

Supported types: `utf8`, signed integers `int8`/`int16`/`int32`/`int64`, unsigned integers `uint8`/`uint16`/`uint32`/`uint64`, `float32`/`float64`, `bool`, and `date32`. Aliases include `string`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, `boolean`, and `date`. Named CSV inputs share this schema.

Inspect query output columns without executing the query:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data' --describe
```

Inspect logical and physical query plans without running the query:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT city FROM data WHERE amount > 5' --explain
```

Set an execution deadline (in seconds) for query planning, running, and exporting:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT COUNT(*) FROM data' --query-timeout 30
```

The timeout starts after input registration. Exports interrupted by the deadline may leave incomplete output files.

Execute a query with physical operator metrics using `--explain-analyze`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT SUM(amount) FROM data' --explain-analyze
```

Unlike `--explain`, this mode runs the query to collect execution metrics.

Export a query result as a single Apache Arrow IPC file for downstream Arrow
tools, without collecting all result batches in memory:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql 'SELECT city, amount FROM data' --output-arrow /tmp/sales.arrow
```

The Arrow output is always one file. It preserves Arrow column types and
supports empty results. Like other exports, a failed or timed-out query can
leave a partial output file.
