# service error and latency summary

Report error counts, slow requests, and maximum request latency per service.

Slow means strictly over 1,000 milliseconds. Errors are records with status `error`.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/service_reliability/data.csv \
  --table events --csv-schema 'service:utf8,request_id:int64,latency_ms:int64,status:utf8' \
  --sql-file examples/analytics/service_reliability/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
