# tune single-partition hash-join byte cutoff

Use `--hash-join-single-partition-mb N` where N is in MiB. This configures DataFusion's `optimizer.hash_join_single_partition_threshold`.

Small build sides can avoid distributed partitioning overhead. A large cutoff risks concentrating memory and work on one partition.

No behavior changes without the CLI flag. Compare release-mode `--explain-analyze` plans and timings on representative large Parquet tables. Confirm identical query results before relying on an optimization.
