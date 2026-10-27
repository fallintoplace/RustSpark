# regional customer lifetime value

Aggregate order revenue per customer before producing regional customer and revenue summaries.

This query assumes that a customer's region is stable. Currency units must match across inputs.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/customer_lifetime_value/data.csv \
  --table events --csv-schema 'customer_id:int64,region:utf8,order_id:int64,amount:int64' \
  --sql-file examples/analytics/customer_lifetime_value/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
