# tune dense small-key perfect hash joins

Use `--perfect-hash-small-build-rows N` where N is in key-range entries. This configures DataFusion's `execution.perfect_hash_join_small_build_threshold`.

Dense integer-key joins can use direct-address hash tables rather than generic hashing; expanding the range increases memory cost.

No behavior changes without the CLI flag. Compare release-mode `--explain-analyze` plans and timings on representative large Parquet tables. Confirm identical query results before relying on an optimization.
