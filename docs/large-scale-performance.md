# Large-scale analytical performance tuning

RustSpark is a DataFusion-based local engine. Billion-row queries completing in seconds require selective scans, appropriate Parquet data layout, ample bandwidth, and workload-specific evidence. These switches are **opt-in**, keep existing defaults, and are not performance guarantees.

Start with `--target-partitions N`, typed Parquet partitions, and `--parquet-pushdown-filters --parquet-reorder-filters` for selective scans. Prefer pre-partitioned Parquet over raw CSV for repeated large-scale analytics. Measure with the repository TPC-H and ClickBench harnesses, checking result correctness as well as median and p95 latency.

## Available tuning knobs

- `--parquet-in-list-pruning-limit 128`: consider up to 128 literal values for Parquet min/max file, row-group, and page pruning. Larger lists add planning and pruning work; compare against default.
- `--parquet-predicate-cache-bytes 67108864`: Cache predicate evaluations during pushdown, trading memory for reduced CPU and I/O; requires --parquet-pushdown-filters.
- `--parquet-metadata-hint-bytes 1048576`: Read this many bytes optimistically at the end of each Parquet file; tune for footer sizes, do not assume a larger hint is faster.
- `--parquet-force-row-selection`: Force RowSelection for Parquet filter results with pushdown enabled. May regress dense filters.
- `--parquet-write-bloom-filters`: Write Parquet bloom filters when the DataFusion writer uses session settings. More storage and write CPU; benefit depends on filter selectivity.
- `--parquet-row-group-rows 131072`: Set rows per written Parquet row group. Smaller groups may prune better but increase footer size and overhead.
- `--parquet-page-size-bytes 65536`: Set best-effort Parquet data-page bytes for new files. Compression and metadata overhead can increase.
- `--parquet-page-row-limit 4096`: Limit rows per Parquet data page for new files. Smaller pages can increase metadata and encoding overhead.
- `--parquet-write-batch-rows 16384`: Set Parquet writer batch rows. Larger batches can consume more memory.
- `--parquet-row-group-writers 4`: Set parallel row-group writers for Parquet export. High concurrency increases memory use.
- `--parquet-write-buffered-batches 8`: Maximum buffered record batches per Parquet stream. Increasing this uses more memory.
- `--perfect-hash-key-range 65536`: Allow perfect-hash joins for smaller build-side integer key ranges. Only helps eligible joins; a larger threshold may waste memory.
- `--hash-join-probe-buffer-bytes 4194304`: Set bytes buffered on hash-join probe side during concurrent build. Increasing it consumes more RAM.
- `--join-inlist-pushdown-bytes 262144`: Increase per-partition join build-side bytes eligible for dynamic IN-list pushdown. More memory and filter setup cost.
- `--join-inlist-pushdown-values 256`: Limit distinct join keys pushed as IN lists. High values cost more planning and probe memory.
- `--sort-in-place-bytes 8388608`: Sort data in a single RecordBatch below this byte threshold. Higher values can increase peak memory.
- `--window-topn`: Use PartitionedTopK for filtered ROW_NUMBER windows. Can regress high-cardinality partitions.
