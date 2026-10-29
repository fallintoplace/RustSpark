# RustSpark native engine

RustSpark now uses this **independent, DataFusion-free Rust query engine** for
its default binary and public library exports. The legacy DataFusion backend,
its unsupported integration tests, and its Ballista example are retired.

## Run the default RustSpark CLI

From the repository root:

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city'
```

Or run the native package directly:

```sh
cargo run --manifest-path native-engine/Cargo.toml -- \
  --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city'
```

## Supported native components

- Typed batches, expressions, native SQL parsing, logical plans and execution
- Filtering, projection, aggregation, sorting, DISTINCT and LIMIT/OFFSET
- Equality-based INNER and LEFT JOIN, plus UNION ALL
- Native multi-table CSV/Parquet CLI input, CSV/JSON Lines results, and bounded
  streaming for simple scan/filter/project CSV queries
- Native JSON extraction, regular expressions and Unicode text functions
- External sorting and disk spill in the `rustspark-stream` CLI
- Arrow record batch conversion and bounded Parquet scan as **library modules**
- Query cancellation, deadlines, scheduling and other experimental scale modules

`rustspark-stream` example:

```sh
cargo run --manifest-path native-engine/Cargo.toml --bin rustspark-stream -- \
  --csv tests/fixtures/sales.csv --schema 'city:text,amount:int' \
  --table sales --sql 'SELECT city, amount FROM sales ORDER BY amount DESC LIMIT 10'
```

This is experimental. SQL support and CLI input formats are narrower than the
retired implementation: no equivalent persistent SQL database, distributed
execution, JSON input or general streaming Parquet SQL CLI yet.

## Checks

```sh
cargo test --all-targets
cargo test --manifest-path native-engine/Cargo.toml --all-targets
cargo clippy --all-targets -- -D warnings
cargo clippy --manifest-path native-engine/Cargo.toml --all-targets -- -D warnings
```

Do not claim Spark parity or better performance without fresh differential
correctness testing and repeatable TPC-H / ClickBench measurements of **this**
native engine. Old DataFusion benchmark results are not comparable.
