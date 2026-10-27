# recency-frequency-value customer segments

Classify customers from recency, order count, and revenue at a fixed snapshot day.

The query uses snapshot day 100. Values are examples, not calibrated business thresholds.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/rfm_segments/data.csv \
  --table events --csv-schema 'customer_id:int64,day:int64,amount:int64' \
  --sql-file examples/analytics/rfm_segments/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
