# subscription MRR snapshots

Measure paying subscriber counts and monthly recurring revenue from monthly snapshots.

Each row is a subscriber-month snapshot, not a payment event.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/subscription_mrr/data.csv \
  --table events --csv-schema 'customer_id:int64,month:utf8,plan:utf8,monthly_fee:int64,is_active:int64' \
  --sql-file examples/analytics/subscription_mrr/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
