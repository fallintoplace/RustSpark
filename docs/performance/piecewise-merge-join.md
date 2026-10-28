# Piecewise merge join planning

Enable with `--piecewise-merge-join`.

**What:** Consider piecewise merge joins for suitable sorted inequality joins instead of less efficient alternatives.

**Tradeoff:** Effect depends on join predicates and ordering, so benchmark against the existing plan.

Run a release build with `--explain-analyze` to confirm the plan and measure before/after using a representative Parquet dataset. Default behavior is unchanged. This is a workload-specific tuning option, not a guarantee that billions of rows will finish in seconds.
