# RustSpark native engine

This is a separate Rust crate for the planned **DataFusion-free execution path**.
It must never import `datafusion` or depend on the root `rustspark` crate.

The first migration phase builds and tests our own schema, typed columnar batches,
expression evaluation, logical plans, physical execution and a small SQL frontend.
The existing DataFusion-powered CLI is unchanged while this backend matures.

Run the native checks from the repository root:

```sh
cargo test --manifest-path native-engine/Cargo.toml --all-targets
```

## Release gates

1. Differential correctness against the existing engine for supported queries,
   including NULLs, overflow, joins and ordering.
2. Bounded memory use, cancellation, query deadlines and disk spill support.
3. Arrow/Parquet integration and compatible CSV/JSON ingestion.
4. TPC-H and ClickBench result checks with comparable timing protocols.
5. Port the public API and CLI, then remove DataFusion and Ballista dependencies.

Do not claim Spark parity or better-than-Spark performance until these gates pass.
The native crate is experimental and is not the default RustSpark engine.

## Run the native CLI

```sh
cargo run --manifest-path native-engine/Cargo.toml -- --csv tests/fixtures/sales.csv --table sales --sql "SELECT city, SUM(amount) AS total FROM sales GROUP BY city ORDER BY city"
```

The native CLI runs without DataFusion. It supports a deliberately restricted
SQL subset, including native joins, grouping, and distinct set operations.
It can read CSV and Parquet, but the primary CLI still materializes input in
memory and cannot use distributed execution. Parquet input is capped at
1,000,000 rows; unsupported Arrow column types and empty Parquet data fail
with explicit errors. The separate streaming CSV CLI has different limits.

Query a Parquet file through the native engine:

```sh
cargo run --manifest-path native-engine/Cargo.toml -- --parquet /data/sales.parquet --table sales --sql 'SELECT city, SUM(amount) AS total FROM sales GROUP BY city'
```

Use exactly one of `--csv` or `--parquet`. The `--stream` option currently
requires CSV and is not a Parquet streaming feature.

## Streaming SQL with external sorting

The `rustspark-stream` native binary executes SELECT, WHERE, projection,
LIMIT/OFFSET and ORDER BY over bounded CSV batches without DataFusion.
ORDER BY writes sorted spill runs and merges them in multiple passes when
needed. Source schema must be explicit. For example:

    cargo run --manifest-path native-engine/Cargo.toml --bin rustspark-stream -- --csv tests/fixtures/sales.csv --schema 'city:text,amount:int' --table sales --sql 'SELECT city, amount FROM sales ORDER BY amount DESC LIMIT 10'

The implementation is experimental. Memory limits, cancellation, SQL
compatibility, Parquet scans, high-cardinality grouping and distributed
execution still need production validation before the old engine is removed.
