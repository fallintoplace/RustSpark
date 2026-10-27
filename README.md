# RustSpark

Experimental local SQL over CSV and Parquet files using Apache DataFusion. Not Spark-compatible.

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales --sql 'SELECT city, SUM(amount) FROM sales GROUP BY city'
```

Automatically choose CSV, JSON (including .jsonl/.ndjson), or Parquet for a file:

```sh
cargo run -- --input tests/fixtures/sales.csv --sql 'SELECT COUNT(*) FROM data'
```

Use the explicit --csv, --json, or --parquet option for directories.

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

Start an interactive SQL shell to run multiple statements in the same session:

```sh
cargo run -- --csv tests/fixtures/sales.csv --interactive
```

Enter SQL statements ending with `;`. Multi-line statements are supported.
Use `\\q` or `.quit` to exit; SQL errors do not close the shell.
The `--query-timeout` applies to each executed statement.
Run `.tables` to list registered tables, `.schemas` to list available catalog
schemas, or `.schema data` to inspect a table's column types without scanning
its rows. These commands work between SQL statements.

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

The Rust API also supports typed, lazy joins over registered tables. It
uses DataFusion's join planner rather than collecting the inputs:

```rust
use datafusion::logical_expr::JoinType;

let joined = session
    .join_tables("left", "right", JoinType::Inner, &[("city", "city")])
    .await?;
let row_count = joined.count().await?;
```

Get a lazy row window from a registered table with `slice_rows`. Sort
the table first when row order matters:

```rust
let page = session.slice_rows("sales", 10, 20).await?;
let rows = page.collect().await?;
```

The call builds a DataFusion LIMIT/OFFSET plan, without collecting the source.

Select the top N rows by one or more columns without collecting the source:

```rust
let top = session.top_rows("sales", &[("amount", false)], 5).await?;
```

Use `true` for ascending and `false` for descending order. Add a unique
tie-breaker when values can be equal.

Use multiple key pairs for compound equality joins. Supported types include
INNER, LEFT, RIGHT, FULL, SEMI, and ANTI. Unmatched outer rows contain NULLs;
NULL join keys do not match other NULL keys by default. A Cartesian product
must be requested explicitly using SQL `CROSS JOIN`.

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

RustSpark's table set operations also support duplicate-preserving
`intersect_all_tables` and `except_all_tables`, along with
`symmetric_difference_tables` for distinct rows found in only one input.
All return lazy DataFrames and compare columns by position.

To remove duplicate rows after matching columns by name, use
`--union-distinct-by-name` instead:

```sh
cargo run -- --csv-table first=tests/fixtures/sales.csv \
  --csv-table second=tests/fixtures/sales.csv \
  --union-distinct-by-name unique_sales=first,second \
  --sql 'SELECT COUNT(*) FROM unique_sales'
```

Missing columns are filled with NULL and duplicates are removed across all
sources. Union destination names must be unique across both union flags.

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

This is an experimental, single-writer local store, not an ACID database.
Persistent `INSERT INTO` copies existing Parquet files into a private stage,
executes the insert there and publishes the new table directory only after
success. On a process crash, the next session either restores the old table
or retains the published new table. Existing immutable Parquet parts are hard-linked into a private replacement
snapshot when supported by the filesystem; otherwise they are copied.
This avoids re-copying old table bytes on normal local filesystems, but
publication still processes every part, is single-writer, and is not a full
transaction. Shared hard-linked parts are **not independent backups**:
external corruption or mutation of a shared inode affects both snapshots.
Use external backups for important data. Do not mutate saved Parquet files
in place or use tools that overwrite existing part names. Power-loss fsync
durability and multi-statement transactions are not guaranteed. Dropped
tables are moved out of the catalog before removal. SQL input files are
never deleted.

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

For a standard JSON array instead of newline-delimited JSON, choose
`--stdout-format json-array`. The result is streamed as one valid JSON array,
preserves explicit NULL fields, and emits `[]` for empty results:

```sh
cargo run -- --sql 'SELECT 1 AS id UNION ALL SELECT 2' --stdout-format json-array
```

Render query results as a Markdown table for reports and GitHub comments:

```sh
cargo run -- --sql "SELECT 42 AS answer" --stdout-format markdown
```

Markdown output streams by record batch, includes headers even for empty results,
and prints SQL NULL values as `NULL`. Pipes and line breaks are escaped.

Export a query result as a single Apache Arrow IPC file for downstream Arrow
tools, without collecting all result batches in memory:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql 'SELECT city, amount FROM data' --output-arrow /tmp/sales.arrow
```

The Arrow output is always one file. It preserves Arrow column types and
supports empty results. Like other exports, a failed or timed-out query can
leave a partial output file.

For wide queries, print one column per line instead of a horizontal table:

```sh
cargo run -- --sql "SELECT 42 AS answer" --stdout-format vertical
```

Vertical output streams one row at a time, displays NULL explicitly, and
escapes embedded line breaks and tabs. Empty queries emit no rows.

To omit the header row in CSV or TSV pipelines, add `--stdout-no-header`
alongside `--stdout-format csv` or `--stdout-format tsv`. This also
produces empty stdout for a query returning no rows.

Stream tab-separated output for spreadsheet and Unix tool pipelines.
Like CSV, TSV writes one header row and escapes fields when needed:

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'SELECT city, amount FROM data ORDER BY amount' --stdout-format tsv
```


### Back up a persistent RustSpark database

Use a database-wide, read-consistent file snapshot without running a SQL query:

```sh
cargo run -- --database-dir /tmp/rustspark-db --backup-database /tmp/rustspark-snapshot
```

The backup copies and validates each persistent table's Parquet parts while
holding RustSpark's exclusive database lock. The destination must be new and
outside the source database. All tables are staged under a temporary sibling
directory and published together by rename after successful copying. An
`.rustspark-backup-manifest` identifies the backup format and table names.
Copies are independent, **not hard links**. To inspect a backup, point
`--database-dir` to its directory.

This is a locally consistent, best-effort backup, not an online multi-writer
snapshot or a guarantee against OS power loss. External processes changing the
data directory are not coordinated. Keep multiple independent backups.

### Compact persistent Parquet tables

Repeated INSERT operations leave separate immutable Parquet parts. For
read-heavy local workloads, merge an existing saved table into **one** part:

```sh
cargo run -- --database-dir /tmp/rustspark-db --compact-table events \
  --sql 'SELECT COUNT(*) FROM events'
```

Compaction streams the old table into a private Parquet part and publishes it
using the same crash-recovery swap as persistent INSERT. It preserves the table's
rows and schema, including an empty table. It is opt-in because a single large
part can reduce scan parallelism and requires enough free disk space for both
versions. Keep other programs from mutating the table's files and use external
backups for important data. The database lock prevents simultaneous RustSpark
operations but does not protect against external writers.
