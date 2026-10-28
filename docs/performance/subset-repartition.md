# tune subset-aware repartition threshold

Pass `--subset-repartition-threshold N` where N is a positive integer. This configures DataFusion's `optimizer.subset_repartition_threshold`.

Avoid unnecessary hash repartitioning when existing data partitions already satisfy a subset of grouping or join keys.

Without the flag, defaults are preserved. Confirm on large Parquet datasets using release-mode `--explain-analyze`, query result checks, and repeated measurements. Improvements are workload dependent.
