# RustSpark

Experimental, single-process SQL over local CSV files, powered by Apache DataFusion. Not Spark-compatible or distributed.

Requires Rust 1.95+.

## Run

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales \
  --sql 'SELECT city, SUM(amount) FROM sales GROUP BY city'
```

The default table name is `data`. The CLI prints a result preview. The library exposes `RustSparkSession` for registering CSV files and running SQL.

## Check

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```
