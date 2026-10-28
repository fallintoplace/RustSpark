# tune adaptive partial-aggregation sampling

Pass `--partial-aggregate-probe-rows N` where N is a positive integer. This configures DataFusion's `execution.skip_partial_aggregation_probe_rows_threshold`.

Adaptive partial aggregation can stop spending CPU on high-cardinality grouping when the reduction ratio is too low.

Without the flag, defaults are preserved. Confirm on large Parquet datasets using release-mode `--explain-analyze`, query result checks, and repeated measurements. Improvements are workload dependent.
