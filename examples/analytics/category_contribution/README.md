# category revenue contribution report

Rank categories by revenue and show their contribution to total revenue.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/category_contribution/data.csv \
  --table events --csv-schema 'category:utf8,units:int64,unit_price:int64' \
  --sql-file examples/analytics/category_contribution/query.sql --stdout-format json
```

Replace `data.csv` with a CSV that has the same column names and types.
The query operates on a single input table named `events`.
