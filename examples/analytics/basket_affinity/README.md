# market-basket product pairs

Find co-purchased product pairs by joining orders to themselves.

The fixture has one row per (order, product). Deduplicate repeated product lines before running the query on other inputs.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/basket_affinity/data.csv \
  --table events --csv-schema 'order_id:int64,product:utf8' \
  --sql-file examples/analytics/basket_affinity/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
