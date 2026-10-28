# Statistics-aware file-group sorting

Enable with `--split-file-groups-by-statistics`.

**What:** Clustered files with disjoint value ranges can preserve ordering and skip downstream sorts.

**Tradeoff:** Experimental and depends on accurate Parquet min/max statistics.

Run a release build with `--explain-analyze` to confirm the plan and measure before/after using a representative Parquet dataset. Default behavior is unchanged. This is a workload-specific tuning option, not a guarantee that billions of rows will finish in seconds.
