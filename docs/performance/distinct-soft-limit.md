# DISTINCT soft-limit pushdown

Enable with `--distinct-soft-limit`.

**What:** DISTINCT with LIMIT can stop after enough groups without materializing every group.

**Tradeoff:** Only helps compatible grouped DISTINCT LIMIT plans.

Run a release build with `--explain-analyze` to confirm the plan and measure before/after using a representative Parquet dataset. Default behavior is unchanged. This is a workload-specific tuning option, not a guarantee that billions of rows will finish in seconds.
