# Returning-customer cohorts

Measure how many customers return after their first observed month. The result uses **ever returned after cohort month**, not
next-month retention. Every customer belongs to their first observed month.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/cohort_returning/data.csv \
  --table events --csv-schema 'customer_id:int64,month:utf8' \
  --sql-file examples/analytics/cohort_returning/query.sql --stdout-format json
```

Replace the fixture with a CSV containing `customer_id` and ISO `month`
(`YYYY-MM`) values. The SQL works in read-only mode.
