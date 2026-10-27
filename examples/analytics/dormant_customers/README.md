# customer inactivity classification

Identify customers with no recent activity using a reproducible inactivity cutoff.

The snapshot day is 100, and dormancy begins at 30 days of inactivity.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/dormant_customers/data.csv \
  --table events --csv-schema 'customer_id:int64,day:int64,event_name:utf8' \
  --sql-file examples/analytics/dormant_customers/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
