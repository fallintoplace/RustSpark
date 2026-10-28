# Null-key join filtering

Enable with `--filter-null-join-keys`.

**What:** Avoid probing impossible NULL keys when joining nullable columns to non-nullable columns.

**Tradeoff:** Extra filtering can hurt scans that cannot push predicates down.

Run a release build with `--explain-analyze` to confirm the plan and measure before/after using a representative Parquet dataset. Default behavior is unchanged. This is a workload-specific tuning option, not a guarantee that billions of rows will finish in seconds.
