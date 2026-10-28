# Large-scale analytical performance tuning

RustSpark is a DataFusion-based local engine. Billion-row queries completing in seconds require selective scans, appropriate Parquet data layout, ample bandwidth, and workload-specific evidence. These switches are **opt-in**, keep existing defaults, and are not performance guarantees.

Start with `--target-partitions N`, typed Parquet partitions, and `--parquet-pushdown-filters --parquet-reorder-filters` for selective scans. Prefer pre-partitioned Parquet over raw CSV for repeated large-scale analytics. Measure with the repository TPC-H and ClickBench harnesses, checking result correctness as well as median and p95 latency.

## Available tuning knobs

- `--parquet-in-list-pruning-limit 128`: consider up to 128 literal values for Parquet min/max file, row-group, and page pruning. Larger lists add planning and pruning work; compare against default.
- `--parquet-predicate-cache-bytes 67108864`: Cache predicate evaluations during pushdown, trading memory for reduced CPU and I/O; requires --parquet-pushdown-filters.
- `--parquet-metadata-hint-bytes 1048576`: Read this many bytes optimistically at the end of each Parquet file; tune for footer sizes, do not assume a larger hint is faster.
- `--parquet-force-row-selection`: Force RowSelection for Parquet filter results with pushdown enabled. May regress dense filters.
- `--parquet-write-bloom-filters`: Write Parquet bloom filters when the DataFusion writer uses session settings. More storage and write CPU; benefit depends on filter selectivity.
