# inventory running-balance query

Trace stock-on-hand by SKU and day, including the point where stock becomes negative.

One row per SKU and day is expected. For multiple movements in a day, add a unique movement ID to the window ordering.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/inventory_running_balance/data.csv \
  --table events --csv-schema 'sku:utf8,day:int64,delta:int64' \
  --sql-file examples/analytics/inventory_running_balance/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
