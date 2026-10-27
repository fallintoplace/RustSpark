# top products per category query

Find the two highest-revenue products in each category with deterministic tie-breaking.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/top_products/data.csv \
  --table events --csv-schema 'category:utf8,product:utf8,units:int64,unit_price:int64' \
  --sql-file examples/analytics/top_products/query.sql --stdout-format json
```

Replace `data.csv` with a CSV that has the same column names and types.
The query operates on a single input table named `events`.
