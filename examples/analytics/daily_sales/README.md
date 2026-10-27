# daily paid-revenue report

Calculate paid order counts and revenue by day without counting refunds or pending orders.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/daily_sales/data.csv \
  --table events --csv-schema 'day:utf8,order_id:int64,status:utf8,amount:int64' \
  --sql-file examples/analytics/daily_sales/query.sql --stdout-format json
```

Replace `data.csv` with a CSV that has the same column names and types.
The query operates on a single input table named `events`.
