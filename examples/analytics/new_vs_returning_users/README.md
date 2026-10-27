# weekly new versus returning users

Separate first-seen users from recurring users in weekly active-user metrics.

Week IDs must sort chronologically. For production, use ISO week identifiers such as `2026-W01`.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/new_vs_returning_users/data.csv \
  --table events --csv-schema 'user_id:int64,week_id:utf8' \
  --sql-file examples/analytics/new_vs_returning_users/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
