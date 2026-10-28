# tune single-partition hash-join row cutoff

Use `--hash-join-single-partition-rows N` where N is in rows. This configures DataFusion's `optimizer.hash_join_single_partition_threshold_rows`.

Changing the row-count cutoff can keep small joins local while repartitioning larger joins across CPU cores.

No behavior changes without the CLI flag. Compare release-mode `--explain-analyze` plans and timings on representative large Parquet tables. Confirm identical query results before relying on an optimization.
