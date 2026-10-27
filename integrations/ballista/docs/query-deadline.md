# Remote query deadline

Set `RUSTSPARK_QUERY_TIMEOUT_SECS` to cap the Ballista client's total time
connecting to the scheduler, registering inputs, planning, executing, and
printing the query. Example:

```sh
BALLISTA_URL=df://scheduler:50050 \
RUSTSPARK_QUERY_TIMEOUT_SECS=120 \
RUSTSPARK_SQL='SELECT COUNT(*) FROM data' \
RUSTSPARK_PARQUET=/shared/events \
cargo run --manifest-path integrations/ballista/Cargo.toml
```

Accepted values are whole seconds from 1 to 86400. Omitting the variable
keeps the existing unbounded behavior.

**This is a client deadline, not distributed cancellation.** Dropping the
client future on timeout does not guarantee the scheduler stops an already
submitted job. Scheduler-side cancellation and server-side deadlines need
their own implementation.
