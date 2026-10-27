# first-touch revenue attribution

Assign purchase revenue to each customer's first observed marketing channel.

This uses a simple first-touch rule and assumes event ordering is stable by day, event name, and channel.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/first_touch_attribution/data.csv \
  --table events --csv-schema 'user_id:int64,day:int64,channel:utf8,event_name:utf8,amount:int64' \
  --sql-file examples/analytics/first_touch_attribution/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
