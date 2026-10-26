# RustSpark

Experimental local SQL over CSV and Parquet files using Apache DataFusion. Not Spark-compatible.

```sh
cargo run -- --csv tests/fixtures/sales.csv --table sales --sql 'SELECT city, SUM(amount) FROM sales GROUP BY city'
```

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

Use a custom CSV delimiter with `--csv-delimiter`:

```sh
cargo run -- --csv tests/fixtures/sales_semicolon.csv --csv-delimiter ';' --sql 'SELECT SUM(amount) FROM data'
```

Join multiple CSV files in one query by naming each input:

```sh
cargo run -- --csv-table left=tests/fixtures/sales.csv --csv-table right=tests/fixtures/sales.csv --sql 'SELECT COUNT(*) FROM left JOIN right USING (city)'
```

Create lazy SQL views for reusable queries. Views can refer to earlier views:

```sh
cargo run -- --csv tests/fixtures/sales.csv \\
  --sql-view 'large=SELECT * FROM data WHERE amount >= 10' \\
  --sql 'SELECT SUM(amount) FROM large'
```

Bound the number of result rows printed with `--max-rows`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data' --max-rows 10
```

Cache a table in memory before running SQL (use only when it fits in RAM):

```sh
cargo run -- --csv tests/fixtures/sales.csv --cache-table data --sql 'SELECT SUM(amount) FROM data'
```

The `--cache-table` flag can be repeated for distinct tables.

Standalone SQL without a file also works:

```sh
cargo run -- --sql 'SELECT 42 AS answer'
```

Provide column types to avoid CSV schema inference:

```sh
cargo run -- --csv tests/fixtures/sales.csv --csv-schema 'city:utf8,amount:int64' --sql 'SELECT SUM(amount) FROM data'
```

Supported types: `utf8`, `int64`, `float64`, `bool` (and `string`, `bigint`, `double`, `boolean` aliases). Named CSV inputs share this schema.

Inspect query output columns without executing the query:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT * FROM data' --describe
```

Inspect logical and physical query plans without running the query:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT city FROM data WHERE amount > 5' --explain
```

Execute a query with physical operator metrics using `--explain-analyze`:

```sh
cargo run -- --csv tests/fixtures/sales.csv --sql 'SELECT SUM(amount) FROM data' --explain-analyze
```

Unlike `--explain`, this mode runs the query to collect execution metrics.
