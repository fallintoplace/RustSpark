# rolling three-day revenue

Compute daily revenue and a three-observation trailing moving total with window frames.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/rolling_revenue/data.csv \
  --table events --csv-schema 'day:utf8,amount:int64' \
  --sql-file examples/analytics/rolling_revenue/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
