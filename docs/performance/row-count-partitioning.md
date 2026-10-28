# Statistics-aware partition planning

Enable with `--row-count-partition-estimates`.

**What:** Use available row-count estimates to make better partitioning decisions on skewed inputs.

**Tradeoff:** Benefits require representative input row-count statistics.

Run a release build with `--explain-analyze` to confirm the plan and measure before/after using a representative Parquet dataset. Default behavior is unchanged. This is a workload-specific tuning option, not a guarantee that billions of rows will finish in seconds.
