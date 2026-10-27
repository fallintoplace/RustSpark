# repeat buyer revenue analysis

Measure repeat buyer counts and their contribution to revenue.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/repeat_buyers/data.csv \
  --table events --csv-schema 'customer_id:int64,order_id:int64,amount:int64' \
  --sql-file examples/analytics/repeat_buyers/query.sql --stdout-format json
```

Replace `data.csv` with a CSV that has the same column names and types.
The query operates on a single input table named `events`.
