# Large-scale analytical performance tuning

RustSpark is a DataFusion-based local engine. Billion-row queries completing in seconds require selective scans, appropriate Parquet data layout, ample bandwidth, and workload-specific evidence. These switches are **opt-in**, keep existing defaults, and are not performance guarantees.

Start with `--target-partitions N`, typed Parquet partitions, and `--parquet-pushdown-filters --parquet-reorder-filters` for selective scans. Prefer pre-partitioned Parquet over raw CSV for repeated large-scale analytics. Measure with the repository TPC-H and ClickBench harnesses, checking result correctness as well as median and p95 latency.

## Available tuning knobs

- `--parquet-statistics-truncate 256`: Set Parquet min/max statistics truncation length on writes. Longer values may improve pruning but increase footer size.
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
- `--prefer-existing-sort`: Use existing input ordering over maximizing repartition parallelism. Can reduce parallel throughput.
- `--split-file-groups-by-statistics`: Experimental statistics-based file grouping for sorted non-overlapping files. Verify correctness and query plans before production.

## Verify billions of rows with a reproducible fixture

The optional benchmark creates deterministic partitioned Parquet with DuckDB (a separate dependency, not needed for normal RustSpark). It requires enough free space and can take a long time to generate at one billion rows. Generation is outside all timed samples.

```sh
python3 -m pip install duckdb
cargo build --release --locked
python3 scripts/quality.py bench billion -- --data-dir /data/rustspark-billion --generate --rows 1000000000 --json-out /data/billion-report.json
```

For short correctness and runner smoke checks, use `--rows 10000` on a fresh dataset root. Subsequent runs omit `--generate` to reuse the files and manifest. The generator writes `id, day, customer_id, amount`, partitioned by `day` with 365 partitions. The benchmark includes a full count, filtered counts, filtered sum and partition maximum. Each sample starts a new process and validates every result against an independent constant-time arithmetic oracle. Reported latency includes process startup and table registration, not only execution. Results are hardware-dependent. The benchmark does **not** demonstrate sub-second or seconds-scale performance until run on actual billion-row data.
