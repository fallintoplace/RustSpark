# push computed leaf expressions toward scans

Use `--leaf-expression-pushdown` to set DataFusion's `optimizer.enable_leaf_expression_pushdown` optimizer option.

Moving nested-field extraction closer to scans can reduce repeated evaluation in joins, filters, and sorts.

This is an opt-in experiment; benchmark on representative large Parquet datasets before enabling it in latency-critical workloads. Confirm that optimized and default query results agree. Defaults are unchanged.
