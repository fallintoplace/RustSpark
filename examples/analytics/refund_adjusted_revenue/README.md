# refund-adjusted monthly revenue

Separate paid revenue from refunds and chargebacks before calculating net revenue.

Amounts represent separate status events, not repeated snapshots of the same order.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/refund_adjusted_revenue/data.csv \
  --table events --csv-schema 'month:utf8,order_id:int64,status:utf8,amount:int64' \
  --sql-file examples/analytics/refund_adjusted_revenue/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
