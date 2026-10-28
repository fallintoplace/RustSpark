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

The native CLI runs without DataFusion. It currently operates on bounded,
in-memory CSV inputs and a deliberately restricted SQL subset. SQL JOIN,
UNION and some other operators are available in the native Rust API, but
not exposed through SQL yet. It does not yet read Parquet or support
disk spill, parallel scheduling, or distributed execution.

## Streaming SQL with external sorting

The `rustspark-stream` native binary executes SELECT, WHERE, projection,
LIMIT/OFFSET and ORDER BY over bounded CSV batches without DataFusion.
ORDER BY writes sorted spill runs and merges them in multiple passes when
needed. Source schema must be explicit. For example:

    cargo run --manifest-path native-engine/Cargo.toml --bin rustspark-stream -- --csv tests/fixtures/sales.csv --schema 'city:text,amount:int' --table sales --sql 'SELECT city, amount FROM sales ORDER BY amount DESC LIMIT 10'

The implementation is experimental. Memory limits, cancellation, SQL
compatibility, Parquet scans, high-cardinality grouping and distributed
execution still need production validation before the old engine is removed.
