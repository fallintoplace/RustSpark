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

Use a custom CSV delimiter with `--csv-delimiter`:

```sh
cargo run -- --csv tests/fixtures/sales_semicolon.csv --csv-delimiter ';' --sql 'SELECT SUM(amount) FROM data'
```

Join multiple CSV files in one query by naming each input:

```sh
cargo run -- --csv-table left=tests/fixtures/sales.csv --csv-table right=tests/fixtures/sales.csv --sql 'SELECT COUNT(*) FROM left JOIN right USING (city)'
```

Bound the number of result rows printed with `--max-rows`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data' --max-rows 10
```
