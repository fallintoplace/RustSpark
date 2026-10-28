# enable optimizer statistics registry

Pass `--use-statistics-registry` to enable this optimization. This configures DataFusion's `optimizer.use_statistics_registry`.

Propagate cardinality estimates across operator boundaries to help the physical planner choose cheaper execution plans.

Without the flag, defaults are preserved. Confirm on large Parquet datasets using release-mode `--explain-analyze`, query result checks, and repeated measurements. Improvements are workload dependent.
