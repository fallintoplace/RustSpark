# combine compatible UNION DISTINCT filters

Use `--union-filter-rewrite` to set DataFusion's `optimizer.enable_unions_to_filter` optimizer option.

Combining compatible UNION DISTINCT branches from the same input can avoid rescanning large tables and evaluating duplicate projections.

This is an opt-in experiment; benchmark on representative large Parquet datasets before enabling it in latency-critical workloads. Confirm that optimized and default query results agree. Defaults are unchanged.
