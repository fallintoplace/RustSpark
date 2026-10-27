# month-over-month revenue deltas

Compare grouped monthly revenue against the prior observed month using LAG.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/revenue_growth/data.csv \
  --table events --csv-schema 'month:utf8,amount:int64' \
  --sql-file examples/analytics/revenue_growth/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
