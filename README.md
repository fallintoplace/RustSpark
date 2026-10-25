# RustSpark

Experimental local SQL over CSV and Parquet files using Apache DataFusion. Not Spark-compatible.

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales --sql 'SELECT city, SUM(amount) FROM sales GROUP BY city'
```

NDJSON files can also be queried directly:

```sh
cargo run -- --json tests/fixtures/sales.json --sql 'SELECT COUNT(*) FROM data'
```

SQL statements can be read from a file:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql-file tests/fixtures/count_sales.sql
```
