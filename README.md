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

Union named inputs by column name, filling missing columns with NULL.
The `--union-by-name` flag accepts existing tables and earlier unions.
SQL views can query the combined result:

```sh
cargo run -- --csv-table first=tests/fixtures/sales.csv \
  --csv-table second=tests/fixtures/sales.csv \
  --union-by-name combined=first,second \
  --sql 'SELECT city, SUM(amount) FROM combined GROUP BY city'
```

Column order can differ between inputs. Rows are appended without deduplication.

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

Materialize expensive SQL once for repeated reads. Materialized views are
registered after ordinary `--sql-view` declarations and can refer to them:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql-view 'large=SELECT * FROM data WHERE amount >= 10' \
  --materialized-sql-view 'totals=SELECT city, SUM(amount) AS total FROM large GROUP BY city' \
  --sql 'SELECT * FROM totals ORDER BY city'
```

Materialized views cache their results in memory at registration, so they may
use significant RAM. Unlike ordinary views, later queries do not rescan the
source files.

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

Persist a query result as a Parquet-backed SQL table. Later processes automatically
load the tables from the same directory:

```sh
cargo run -- --csv tests/fixtures/sales.csv --database-dir /tmp/rustspark-db \\
  --save-table sales_summary --sql 'SELECT city, SUM(amount) AS total FROM data GROUP BY city'
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'SELECT * FROM sales_summary ORDER BY city'
```

Saved tables are written to a staging directory and published after the export
succeeds. Existing table names are rejected, and unfinished staging directories
are ignored at startup. With `--database-dir`, SQL `CREATE TABLE AS SELECT`
creates a persistent table. SQL `INSERT INTO` can append rows to it:

```sh
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'CREATE TABLE numbers AS SELECT 1 AS n; INSERT INTO numbers VALUES (2);'
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'SELECT SUM(n) FROM numbers'
```

An empty typed table can also be created and populated later:

```sh
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'CREATE TABLE people (id BIGINT, name VARCHAR); INSERT INTO people VALUES (1, \'Ada\');'
```

Drop a saved table, including its on-disk Parquet files, or use `DROP TABLE IF EXISTS`
when it may not exist:

```sh
cargo run -- --database-dir /tmp/rustspark-db --sql 'DROP TABLE numbers'
```

Rename a saved table without copying its rows:

```sh
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'ALTER TABLE people RENAME TO customers'
```

Only simple, unquoted table names in the local database are supported for
persistent rename. Existing table names cannot be overwritten.

This is an experimental, local append-only store. `INSERT INTO` writes Parquet
files; it is not a transactional database. Dropped table files are moved out of
the catalog before removal. SQL data in input files is never deleted.

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

Supported positional and named numeric parameter types also include `int8`, `int16`,
`int32`, `uint8`, `uint16`, `uint32`, `uint64`, and `float32`.

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

Stream query results to standard output for shell pipelines, without collecting
the full result in memory. `--stdout-format csv` writes CSV with one header;
`--stdout-format json` writes one JSON object per line (NDJSON), including
explicit null fields. `--max-rows` and `--offset` work with both formats:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'SELECT city, amount FROM data ORDER BY amount' \
  --stdout-format json --max-rows 2
```

Pipe typed query results to another Arrow-aware process with the IPC stream
format. This writes binary output and keeps row batches bounded in memory:

```sh
cargo run -- --sql 'SELECT 42 AS answer' --stdout-format arrow > /tmp/query.stream
```

This is the Arrow IPC **stream** format, not the seekable IPC file format
written by `--output-arrow`.

Export a query result as a single Apache Arrow IPC file for downstream Arrow
tools, without collecting all result batches in memory:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql 'SELECT city, amount FROM data' --output-arrow /tmp/sales.arrow
```

The Arrow output is always one file. It preserves Arrow column types and
supports empty results. Like other exports, a failed or timed-out query can
leave a partial output file.

Stream tab-separated output for spreadsheet and Unix tool pipelines.
Like CSV, TSV writes one header row and escapes fields when needed:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'SELECT city, amount FROM data ORDER BY amount' --stdout-format tsv
```
