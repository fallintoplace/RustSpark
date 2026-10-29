# RustSpark

An **experimental native SQL query engine written in Rust**. The default `rustspark`
command and Rust library now use RustSpark's own SQL parser, logical plan,
execution operators, and columnar batches. **No DataFusion dependency or fallback.**

This is a breaking replacement of the previous DataFusion-based CLI and
`RustSparkSession` API. The native engine is not feature-equivalent to the
previous release or Apache Spark. Unsupported syntax produces an explicit error.

## Run a query

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city'
```

Or read SQL from a file:

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql-file tests/fixtures/count_sales.sql
```

The default command returns CSV, or typed JSON Lines with `--jsonl`. To run
native SQL across multiple CSV or Parquet files, register each table with a
repeatable `--source TABLE=FILE` argument:

```sh
cargo run -- --source q1=tests/fixtures/sales.csv \
  --source q2=tests/fixtures/sales.csv \
  --sql 'SELECT city FROM q1 UNION ALL SELECT city FROM q2' --jsonl
```

The in-memory native CLI supports a limited SQL subset, including projections,
filters, grouping, aggregates, sorting, LIMIT/OFFSET, equality joins and UNION ALL.
The `--stream` option remains limited to a single CSV source and simple operators.

## Native streaming

For bounded-memory scans, filters, projections and LIMIT:

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city FROM sales LIMIT 5' --stream \
  --csv-schema 'city:text,amount:int64'
```

For streaming ORDER BY with external sort and disk spill, use the separately
supported `rustspark-stream` binary:

```sh
cargo run --manifest-path native-engine/Cargo.toml --bin rustspark-stream -- \
  --csv tests/fixtures/sales.csv --table sales \
  --schema 'city:text,amount:int' \
  --sql 'SELECT city, amount FROM sales ORDER BY amount DESC LIMIT 10'
```

The native engine also contains Arrow conversion and Parquet batch-scanning
modules. Parquet is supported in the in-memory CLI with an explicit row limit;
the streaming CLI still has narrower input and operator support. Full JSON
input, interactive, persistent and distributed modes are not yet replaced.

## Development

```sh
cargo test --all-targets
cargo test --manifest-path native-engine/Cargo.toml --all-targets
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path native-engine/Cargo.toml --all-targets -- -D warnings
```

The native engine implementation lives in [native-engine/](native-engine/).
Prior benchmarks and documentation describing the DataFusion-backed engine
are historical and **must not be treated as measurements of this native CLI**.

See [native-engine/README.md](native-engine/README.md) for backend design and
limitations.
