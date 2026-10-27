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

In interactive mode, use `.read path/to/queries.sql` to run SQL from a
file in the current session. The path may contain spaces. Query errors do
not close the shell.

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
Use `.count data` to print the number of rows in a registered table.

Use `.preview data` to display up to 10 rows from a table or
`.preview data 3` to set a limit of 3 rows (maximum 1000).
Qualified names such as `.preview datafusion.public.data 3` also work.

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

Get distinct values from one column without collecting all rows:

```rust
let cities = session.distinct_column_values("sales", "city").await?;
```

The result is lazy and includes at most one NULL. Sort it when ordering matters.

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

Count NULL values per column with a lazy aggregate:

```rust
let missing = session.null_counts("sales", &["city", "amount"]).await?;
```

Pass an empty column slice to count NULLs across every column.


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

Increase file metadata concurrency when loading directories with many files:

```sh
cargo run -- --parquet /data/lake/events --metadata-fetch-concurrency 64 \\
  --sql 'SELECT COUNT(*) FROM data'
```

This controls concurrent file schema and statistics reads. Higher settings can
reduce metadata latency but use more memory and storage I/O. The default is
unchanged when the flag is omitted.

Tune scan splitting for large input files and many available CPU cores:

```sh
cargo run -- --csv /data/large.csv --target-partitions 8 \\
  --scan-split-min-kb 256 --sql 'SELECT SUM(amount) FROM data'
```

The option sets the minimum file-group size that DataFusion considers for
splitting a scan. Smaller values can help fill workers on moderate-size files
but add scheduling and I/O overhead. The default stays unchanged unless the
option is specified. Compressed or unsplittable formats may not benefit.

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

Query many Parquet parts using a file manifest instead of a huge command line:

```sh
# /data/manifests/parts.txt contains one Parquet filename per line.
cargo run -- --parquet-file-list /data/manifests/parts.txt \\
  --sql 'SELECT COUNT(*) FROM data'
```

Each file path is resolved relative to the manifest's directory unless absolute.
Blank lines and lines starting with `#` are ignored. The files must exist,
and the list must contain at least one file. All files must have compatible
schemas. Input files remain lazy; only the list of paths is loaded at startup.
The selected manifest and all listed files are protected from output overwrite.

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

For a fast row-existence check, use the Rust API instead of a full count:

```rust
let has_sales = session.table_has_rows("sales").await?;
```

This reads at most one row and works with registered tables and SQL views.


Persist a query result as a Parquet-backed SQL table. Later processes automatically
load the tables from the same directory:

```sh
cargo run -- --csv tests/fixtures/sales.csv --database-dir /tmp/rustspark-db \\
  --save-table sales_summary --sql 'SELECT city, SUM(amount) AS total FROM data GROUP BY city'
cargo run -- --database-dir /tmp/rustspark-db \\
  --sql 'SELECT * FROM sales_summary ORDER BY city'
```

To choose the compression codec for a saved Parquet table, combine
`--parquet-compression` with `--save-table`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --database-dir /tmp/rustspark-db \
  --save-table compressed_sales --parquet-compression 'zstd(3)' \
  --sql 'SELECT * FROM data'
```

An omitted codec retains DataFusion's default. The flag also continues to
work with `--output-parquet`. It does not change the writer settings for
SQL `CREATE TABLE`, `INSERT INTO`, or `--compact-table`.

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

Choose a disk with enough free space for large-query spill files (sorting and joining):

```sh
cargo run -- --spill-dir /fast-ssd/scratch --memory-limit-mb 512 \\
  --csv /data/large.csv --sql 'SELECT * FROM data ORDER BY amount' --output-parquet /tmp/sorted
```

The spill directory must exist. RustSpark keeps default temporary-file behavior
when the flag is absent and rejects spill directories inside `--database-dir`.
A memory limit is optional, but a limit can trigger external spilling.

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

For HTML reports, stream an embeddable table with escaped cell values:

```sh
cargo run -- --sql 'SELECT 42 AS answer' --stdout-format html
```

NULL cells receive a `null` CSS class. Column names and values are HTML-escaped.

Split very large partitioned exports into manageable part files:

```sh
cargo run -- --csv /data/events.csv --batch-size 4096 \\
  --sql 'SELECT * FROM data' --output-parquet /data/exported \\
  --output-rows-per-file 1000000
```

This is a soft limit (batch boundaries may exceed it). DataFusion may produce
multiple Parquet, CSV or JSON parts, depending on the input partitions.
The same tuning also applies to persistent database writes. It cannot be
combined with `--output-single-file`.

Export a query result as a single Apache Arrow IPC file for downstream Arrow
tools, without collecting all result batches in memory:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql 'SELECT city, amount FROM data' --output-arrow /tmp/sales.arrow
```

The Arrow output is always one file. It preserves Arrow column types and
supports empty results. Like other exports, a failed or timed-out query can
leave a partial output file.

Arrow IPC files can optionally compress record-batch buffers with LZ4 (lower
CPU cost) or ZSTD (often smaller files):

```sh
cargo run -- --csv tests/fixtures/sales.csv \
  --sql 'SELECT * FROM data' --output-arrow /tmp/sales.arrow \
  --arrow-compression zstd
```

`--arrow-compression` applies only to `--output-arrow` files, not Arrow
streams written to stdout. Omitting it preserves the previous uncompressed
file format.

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

## License

RustSpark is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
