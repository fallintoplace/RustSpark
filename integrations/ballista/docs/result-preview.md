# Bounded remote result preview

The optional Ballista client displays only a small preview by default. Use
`RUSTSPARK_MAX_ROWS` to choose an explicit maximum number of result rows
transferred to the client for display:

```sh
BALLISTA_URL=df://scheduler:50050 \
RUSTSPARK_PARQUET=/shared/events \
RUSTSPARK_SQL='SELECT * FROM data ORDER BY timestamp DESC' \
RUSTSPARK_MAX_ROWS=100 \
cargo run --manifest-path integrations/ballista/Cargo.toml
```

The count must be between 1 and 10000. The client uses DataFusion's
`DataFrame::show_limit`, which plans a row limit instead of fetching all
result batches before printing. With no variable set, DataFusion's existing
`show()` default remains unchanged.

This limits **output rows**, not source bytes scanned or memory used by the
distributed query. Expensive sorts, shuffles, and joins may still process the
whole dataset before the result limit is applied. Add SQL filters, partitions,
and an explicit SQL LIMIT when possible.
