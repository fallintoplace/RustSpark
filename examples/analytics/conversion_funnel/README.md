# sequential conversion funnel counts

Count distinct visitors, signups, and purchasers without double-counting repeated events.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/conversion_funnel/data.csv \
  --table events --csv-schema 'user_id:int64,event_name:utf8' \
  --sql-file examples/analytics/conversion_funnel/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
