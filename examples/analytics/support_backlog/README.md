# support backlog and overdue counts

Summarize unresolved and overdue support tickets by team from a point-in-time snapshot.

The query uses day 10 as a snapshot, day 0 to represent an open ticket, and five days as the overdue threshold.

Run from the repository root:

```sh
cargo run -- --csv examples/analytics/support_backlog/data.csv \
  --table events --csv-schema 'ticket_id:int64,team:utf8,opened_day:int64,closed_day:int64' \
  --sql-file examples/analytics/support_backlog/query.sql --stdout-format json
```

Replace the fixture with a CSV using the same column names and types.
The input table is named `events`.
