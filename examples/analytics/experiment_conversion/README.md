# experiment conversion analysis

Compare conversion and non-conversion counts per experiment variant.

Each row represents one unique experiment participant. These counts do not establish statistical significance.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/experiment_conversion/data.csv \
  --table events --csv-schema 'variant:utf8,user_id:int64,converted:int64' \
  --sql-file examples/analytics/experiment_conversion/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
