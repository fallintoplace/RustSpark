# RustSpark

RustSpark is an experimental, Rust-native query engine project inspired by Apache Spark.

**Current scope:** a single-process SQL prototype backed by [Apache DataFusion](https://datafusion.apache.org/). This project does **not** implement Spark's distributed scheduler, Spark Connect protocol, or PySpark compatibility yet.

## Quick start

Requires Rust 1.95 or later.

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) AS revenue FROM sales GROUP BY city ORDER BY city'
```

The CLI registers a local, header-based CSV file as a table, executes one SQL statement, and prints a preview of the result. The table name defaults to `data`, so `--table` is optional. Run `cargo run -- --help` for options.

You can also embed the session in Rust:

```rust
use rustspark::RustSparkSession;

# async fn example() -> datafusion::error::Result<()> {
let session = RustSparkSession::new();
session.register_csv("sales", "tests/fixtures/sales.csv").await?;
let result = session.sql("SELECT COUNT(*) FROM sales").await?;
result.show().await?;
# Ok(())
# }
```

## Design and next steps

RustSpark uses DataFusion for Arrow-native scanning, SQL planning, and local execution. `RustSparkSession` is a small project-owned boundary for future extensions, not a Spark-compatible API.

Near-term milestones:

1. Add a lazy DataFrame API and tests for local SQL and CSV/Parquet semantics.
2. Define a stable plan/worker boundary and measure query execution before choosing a scheduler.
3. Investigate Spark Connect interoperability separately from the core query engine.

Keeping these steps separate avoids confusing a useful local query engine with a distributed, Spark-compatible system.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
