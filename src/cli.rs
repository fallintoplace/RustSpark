use std::{collections::HashSet, path::PathBuf, sync::Arc};

use clap::{ArgGroup, Parser, ValueEnum};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use rustspark::RustSparkSession;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StdoutFormat {
    Csv,
    Tsv,
    Vertical,
    Json,
    JsonArray,
    Markdown,
    Html,
    Arrow,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ArrowCompression {
    Lz4,
    Zstd,
}

#[derive(Parser)]
#[command(
    version,
    about = "Run local SQL queries over CSV, JSON or Parquet files",
    group(ArgGroup::new("input").required(false).multiple(true).args(["csv", "parquet", "json", "parquet_file_list", "csv_tables", "parquet_tables", "json_tables", "auto_input"])),
    group(ArgGroup::new("query").required(true).args(["sql", "sql_file", "interactive", "backup_database"]))
)]
struct Args {
    /// Infer the input reader from a single file extension.
    #[arg(
        long = "input",
        value_name = "FILE",
        conflicts_with_all = [
            "csv", "parquet", "json", "csv_tables", "parquet_tables", "json_tables"
        ]
    )]
    auto_input: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    csv: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    parquet: Option<PathBuf>,

    /// Load Parquet parts listed one per line, relative to the manifest directory.
    #[arg(long = "parquet-file-list", value_name = "FILE", conflicts_with_all = [
        "csv", "parquet", "json", "auto_input", "csv_tables", "parquet_tables", "json_tables"
    ])]
    parquet_file_list: Option<PathBuf>,

    #[arg(long = "csv-table", value_name = "NAME=PATH")]
    csv_tables: Vec<String>,

    #[arg(long = "parquet-table", value_name = "NAME=PATH")]
    parquet_tables: Vec<String>,

    #[arg(long, value_name = "CHAR", value_parser = parse_delimiter)]
    csv_delimiter: Option<u8>,

    #[arg(long = "csv-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    csv_extension: Option<String>,

    #[arg(long = "parquet-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    parquet_extension: Option<String>,

    #[arg(long = "parquet-partitions", value_name = "NAME:TYPE,...", value_parser = parse_parquet_partitions)]
    parquet_partitions: Option<Schema>,

    #[arg(long = "parquet-pushdown-filters")]
    parquet_pushdown_filters: bool,

    #[arg(
        long = "parquet-reorder-filters",
        requires = "parquet_pushdown_filters"
    )]
    parquet_reorder_filters: bool,

    /// Heap-based ROW_NUMBER TopK optimization; benchmark on high-cardinality keys.
    #[arg(long = "window-topn")]
    window_topn: bool,

    #[arg(long = "parquet-binary-as-string")]
    parquet_binary_as_string: bool,
    /// Tune how many existing partitions are needed before avoiding a subset repartition.
    #[arg(long = "subset-repartition-min-partitions", value_name = "COUNT", value_parser = parse_subset_repartition_min_partitions)]
    subset_repartition_min_partitions: Option<usize>,

    /// Expose the group-to-row ratio that triggers skipping partial aggregation.
    #[arg(long = "partial-agg-skip-ratio", value_name = "RATIO", value_parser = parse_partial_agg_skip_ratio)]
    partial_agg_skip_ratio: Option<f64>,

    /// Control when an aggregate samples group cardinality before bypassing partial aggregation.
    #[arg(long = "partial-agg-probe-rows", value_name = "COUNT", value_parser = parse_partial_agg_probe_rows)]
    partial_agg_probe_rows: Option<usize>,

    /// Control read-ahead memory per partition after sort pushdown.
    #[arg(long = "sort-pushdown-buffer-mb", value_name = "MIB", value_parser = parse_sort_pushdown_buffer_bytes)]
    sort_pushdown_buffer_bytes: Option<usize>,

    /// Expose the threshold for sorting in a single RecordBatch.
    #[arg(long = "sort-in-place-max-mb", value_name = "MIB", value_parser = parse_sort_in_place_threshold_bytes)]
    sort_in_place_threshold_bytes: Option<usize>,

    /// Expose the memory reservation for spillable sort merges.
    #[arg(long = "sort-spill-reserve-mb", value_name = "MIB", value_parser = parse_sort_spill_reserve_bytes)]
    sort_spill_reserve_bytes: Option<usize>,

    /// Tune the build-side integer key range for the perfect-hash fast path.
    #[arg(long = "perfect-hash-small-range", value_name = "COUNT", value_parser = parse_perfect_hash_small_range)]
    perfect_hash_small_range: Option<usize>,

    /// Expose the maximum number of distinct build-side join keys for IN-list dynamic filtering.
    #[arg(long = "join-dynamic-inlist-distinct", value_name = "COUNT", value_parser = parse_join_dynamic_inlist_distinct)]
    join_dynamic_inlist_distinct: Option<usize>,

    /// Tune maximum per-partition IN-list dynamic filter bytes.
    #[arg(long = "join-dynamic-inlist-kb", value_name = "KIB", value_parser = parse_join_dynamic_inlist_bytes)]
    join_dynamic_inlist_bytes: Option<usize>,

    /// Read Parquet strings and binary data as Arrow view types.
    #[arg(long = "parquet-view-arrays")]
    parquet_view_arrays: bool,

    /// Force row selections for Parquet filter pushdown instead of auto choice.
    #[arg(long = "parquet-force-filter-selections", requires = "parquet_pushdown_filters")]
    parquet_force_filter_selections: bool,

    /// Predicate cache capacity in MiB for selective Parquet decoding.
    #[arg(long = "parquet-predicate-cache-mb", value_name = "MIB", value_parser = parse_memory_limit_mib, requires = "parquet_pushdown_filters")]
    parquet_predicate_cache_mb_bytes: Option<usize>,

    /// Bytes to prefetch from each Parquet footer, supplied in KiB.
    #[arg(long = "parquet-metadata-hint-kb", value_name = "KIB", value_parser = parse_scan_split_min_kb)]
    parquet_metadata_hint_kb_bytes: Option<usize>,

    /// Allow larger literal IN lists to participate in Parquet pruning.
    #[arg(long = "parquet-max-in-list-size", value_name = "VALUES", value_parser = parse_target_partitions)]
    parquet_max_in_list_size: Option<usize>,

    /// Read Parquet Bloom filters to skip row groups for selective predicates.
    #[arg(long = "parquet-bloom-filters")]
    parquet_bloom_filters: bool,


    #[arg(long = "csv-quote", value_name = "CHAR", value_parser = parse_csv_char)]
    csv_quote: Option<u8>,

    #[arg(long = "csv-escape", value_name = "CHAR", value_parser = parse_csv_char)]
    csv_escape: Option<u8>,

    #[arg(long = "csv-multiline")]
    csv_multiline: bool,

    #[arg(long = "csv-allow-short-rows")]
    csv_allow_short_rows: bool,

    #[arg(long = "csv-no-header")]
    csv_no_header: bool,

    #[arg(long = "csv-comment", value_name = "CHAR", value_parser = parse_delimiter)]
    csv_comment: Option<u8>,

    #[arg(long = "csv-null-value", value_name = "TEXT", value_parser = parse_csv_null_value)]
    csv_null_value: Option<String>,

    #[arg(long = "csv-schema", value_name = "NAME:TYPE,...", value_parser = parse_csv_schema)]
    csv_schema: Option<Schema>,

    #[arg(long = "csv-schema-rows", value_name = "ROWS", value_parser = parse_csv_schema_rows)]
    csv_schema_rows: Option<usize>,

    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,

    #[arg(long = "json-schema", value_name = "NAME:TYPE,...", value_parser = parse_json_schema)]
    json_schema: Option<Schema>,

    #[arg(long = "json-schema-rows", value_name = "ROWS", value_parser = parse_json_schema_rows)]
    json_schema_rows: Option<usize>,

    #[arg(long = "json-extension", value_name = ".EXT", value_parser = parse_file_extension)]
    json_extension: Option<String>,

    #[arg(long = "json-array")]
    json_array: bool,

    #[arg(long = "json-table", value_name = "NAME=PATH")]
    json_tables: Vec<String>,

    #[arg(long, default_value = "data", value_parser = parse_table_name, conflicts_with_all = ["csv_tables", "parquet_tables", "json_tables"])]
    table: String,

    #[arg(long = "cache-table", value_name = "NAME")]
    cache_tables: Vec<String>,

    #[arg(long = "union-by-name", value_name = "NAME=TABLE1,TABLE2,...")]
    unions_by_name: Vec<String>,

    #[arg(long = "union-distinct-by-name", value_name = "NAME=TABLE1,TABLE2,...")]
    unions_distinct_by_name: Vec<String>,

    #[arg(long, value_name = "QUERY")]
    sql: Option<String>,

    #[arg(long, value_name = "PATH")]
    sql_file: Option<PathBuf>,

    /// Load saved Parquet tables from this directory on each run.
    #[arg(long = "database-dir", value_name = "DIR")]
    database_dir: Option<PathBuf>,

    /// Save the final SQL query as a new Parquet table in --database-dir.
    #[arg(
        long = "save-table",
        value_name = "NAME",
        value_parser = parse_table_name,
        requires = "database_dir",
        conflicts_with_all = [
            "describe", "explain", "explain_analyze", "count_only", "max_rows",
            "stdout_format", "output_parquet", "output_csv", "output_json",
            "output_arrow", "output_single_file"
        ]
    )]
    save_table: Option<String>,

    /// Compact one saved Parquet table into a single part before running SQL.
    /// Uses a private replacement and the normal crash-recovery protocol.
    #[arg(
        long = "compact-table",
        value_name = "NAME",
        value_parser = parse_table_name,
        requires = "database_dir",
        conflicts_with = "backup_database"
    )]
    compact_table: Option<String>,

    /// Copy a consistent snapshot of all saved Parquet tables to a new directory.
    /// No SQL statement is required for this operation.
    #[arg(
        long = "backup-database",
        value_name = "DIR",
        requires = "database_dir"
    )]
    backup_database: Option<PathBuf>,

    /// Start a SQL shell that executes semicolon-terminated statements.
    #[arg(long)]
    interactive: bool,

    #[arg(long = "sql-view", value_name = "NAME=SQL")]
    sql_views: Vec<String>,

    #[arg(long = "materialized-sql-view", value_name = "NAME=SQL")]
    materialized_sql_views: Vec<String>,

    #[arg(long = "param", value_name = "TYPE:VALUE", value_parser = parse_sql_param)]
    params: Vec<ScalarValue>,

    #[arg(
        long = "named-param",
        value_name = "NAME=TYPE:VALUE",
        value_parser = parse_named_param,
        conflicts_with = "params"
    )]
    named_params: Vec<(String, ScalarValue)>,

    #[arg(
        long = "describe",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    describe: bool,

    #[arg(
        long = "explain",
        conflicts_with_all = ["describe", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    explain: bool,

    #[arg(
        long = "explain-analyze",
        conflicts_with_all = ["describe", "explain", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    explain_analyze: bool,

    #[arg(long, value_name = "ROWS", value_parser = parse_positive_rows)]
    max_rows: Option<usize>,

    #[arg(long, value_name = "ROWS", requires = "max_rows")]
    offset: Option<usize>,

    #[arg(
        long = "stdout-format",
        value_enum,
        value_name = "FORMAT",
        conflicts_with_all = ["describe", "explain", "explain_analyze", "count_only", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    stdout_format: Option<StdoutFormat>,

    /// Do not print CSV or TSV column names before result rows.
    #[arg(long = "stdout-no-header", requires = "stdout_format")]
    stdout_no_header: bool,

    #[arg(
        long = "count-only",
        conflicts_with_all = ["describe", "explain", "explain_analyze", "max_rows", "output_parquet", "output_csv", "output_json", "output_arrow"]
    )]
    count_only: bool,

    #[arg(long = "query-timeout", value_name = "SECONDS", value_parser = parse_query_timeout)]
    query_timeout: Option<u64>,

    /// DISTINCT soft-limit pushdown (opt-in for benchmarking).
    #[arg(long = "distinct-soft-limit")]
    distinct_soft_limit: bool,

    #[arg(long = "memory-limit-mb", value_name = "MIB", value_parser = parse_memory_limit_mib)]
    memory_limit_bytes: Option<usize>,

    /// Null-key join filtering (opt-in for benchmarking).
    #[arg(long = "filter-null-join-keys")]
    filter_null_join_keys: bool,

    /// Configure tune hash-join dynamic IN-list byte budget in KiB.
    #[arg(
        long = "hash-join-inlist-max-kb",
        value_name = "SIZE",
        value_parser = parse_hash_join_inlist_max_bytes
    )]
    hash_join_inlist_max_bytes: Option<usize>,

    /// Configure tune dynamic hash-join IN-list cardinality in values.
    #[arg(
        long = "hash-join-inlist-max-values",
        value_name = "SIZE",
        value_parser = parse_hash_join_inlist_max_values
    )]
    hash_join_inlist_max_values: Option<usize>,

    /// tune single-partition hash-join byte cutoff, in MiB.
    #[arg(
        long = "hash-join-single-partition-mb",
        value_name = "SIZE",
        value_parser = parse_hash_join_single_partition_bytes
    )]
    hash_join_single_partition_bytes: Option<usize>,

    /// tune single-partition hash-join row cutoff, in rows.
    #[arg(
        long = "hash-join-single-partition-rows",
        value_name = "SIZE",
        value_parser = parse_hash_join_single_partition_rows
    )]
    hash_join_single_partition_rows: Option<usize>,

    /// tune dense small-key perfect hash joins, in key-range entries.
    #[arg(
        long = "perfect-hash-small-build-rows",
        value_name = "SIZE",
        value_parser = parse_perfect_hash_small_build_rows
    )]
    perfect_hash_small_build_rows: Option<usize>,

    /// tune sort-pushdown scan read-ahead, measured in MiB.
    #[arg(
        long = "sort-read-ahead-mb",
        value_name = "SIZE",
        value_parser = parse_sort_read_ahead_bytes
    )]
    sort_read_ahead_bytes: Option<usize>,

    /// tune multi-branch SQL planning concurrency, measured in concurrent planning tasks.
    #[arg(
        long = "planning-concurrency",
        value_name = "SIZE",
        value_parser = parse_planning_concurrency_override
    )]
    planning_concurrency_override: Option<usize>,

    /// tune adaptive partial-aggregation sampling.
    #[arg(
        long = "partial-aggregate-probe-rows",
        value_name = "ROWS",
        value_parser = parse_partial_aggregate_probe_rows
    )]
    partial_aggregate_probe_rows: Option<usize>,

    /// tune subset-aware repartition threshold.
    #[arg(
        long = "subset-repartition-threshold",
        value_name = "ROWS",
        value_parser = parse_subset_repartition_threshold
    )]
    subset_repartition_threshold: Option<usize>,

    /// enable optimizer statistics registry.
    #[arg(long = "use-statistics-registry")]
    use_statistics_registry: bool,

    /// push computed leaf expressions toward scans.
    #[arg(long = "leaf-expression-pushdown")]
    leaf_expression_pushdown: bool,

    /// combine compatible UNION DISTINCT filters.
    #[arg(long = "union-filter-rewrite")]
    union_filter_rewrite: bool,

    /// Configure buffer hash-join probe input while building in MiB.
    #[arg(
        long = "hash-join-probe-buffer-mb",
        value_name = "SIZE",
        value_parser = parse_hash_join_probe_buffer_bytes
    )]
    hash_join_probe_buffer_mb: Option<usize>,

    /// tune in-place sort size threshold, measured in KiB.
    #[arg(
        long = "sort-in-place-kb",
        value_name = "SIZE",
        value_parser = parse_sort_in_place_bytes
    )]
    sort_in_place_kb: Option<usize>,

    /// Put temporary spill files for large queries on a chosen disk.
    #[arg(long = "spill-dir", value_name = "DIR")]
    spill_dir: Option<PathBuf>,

    /// Statistics-aware file-group sorting (opt-in for benchmarking).
    #[arg(long = "split-file-groups-by-statistics")]
    split_file_groups_by_statistics: bool,

    /// Stop preferring hash joins for eligible large joins.
    #[arg(long = "prefer-sort-merge-join")]
    prefer_sort_merge_join: bool,

    /// Disable file-scan work stealing for predictable layouts.
    #[arg(long = "disable-file-work-stealing")]
    disable_file_work_stealing: bool,

    /// Enforce the target RecordBatch size at join outputs.
    #[arg(long = "enforce-join-batch-size")]
    enforce_join_batch_size: bool,

    /// Skip listing-time table statistics for cold-start latency.
    #[arg(long = "skip-table-statistics")]
    skip_table_statistics: bool,

    /// Keep distinct Hive partitions grouped when this many values are present.
    #[arg(
        long = "preserve-file-partitions",
        value_name = "COUNT",
        value_parser = parse_positive_tuning_usize
    )]
    preserve_file_partitions: Option<usize>,

    #[arg(long = "batch-size", value_name = "ROWS", value_parser = parse_batch_size)]
    batch_size: Option<usize>,

    /// Statistics-aware partition planning (opt-in for benchmarking).
    #[arg(long = "row-count-partition-estimates")]
    row_count_partition_estimates: bool,

    #[arg(long = "target-partitions", value_name = "PARTITIONS", value_parser = parse_target_partitions)]
    target_partitions: Option<usize>,

    /// Piecewise merge join planning (opt-in for benchmarking).
    #[arg(long = "piecewise-merge-join")]
    piecewise_merge_join: bool,

    /// Minimum total file-group size before DataFusion splits a scan across workers.
    #[arg(long = "scan-split-min-kb", value_name = "KIB", value_parser = parse_scan_split_min_kb)]
    scan_split_min_bytes: Option<usize>,

    /// Maximum concurrent file metadata reads during schema and statistics discovery.
    #[arg(long = "metadata-fetch-concurrency", value_name = "FILES", value_parser = parse_positive_metadata_concurrency)]
    metadata_fetch_concurrency: Option<usize>,

    /// Maximum values considered for Parquet IN-list statistics pruning.
    #[arg(long = "parquet-in-list-pruning-limit", value_name = "VALUES", value_parser = parse_positive_tuning_usize)]
    parquet_in_list_pruning_limit: Option<usize>,

    /// Opt-in Parquet performance setting; preserves DataFusion defaults otherwise.
    #[arg(long = "parquet-predicate-cache-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_predicate_cache_bytes: Option<usize>,

    /// Opt-in Parquet performance setting; preserves DataFusion defaults otherwise.
    #[arg(long = "parquet-metadata-hint-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_metadata_hint_bytes: Option<usize>,

    /// Opt-in Parquet performance setting; preserves DataFusion defaults otherwise.
    #[arg(long = "parquet-force-row-selection", requires = "parquet_pushdown_filters")]
    parquet_force_row_selection: bool,

    /// Opt-in Parquet performance setting; preserves DataFusion defaults otherwise.
    #[arg(long = "parquet-write-bloom-filters")]
    parquet_write_bloom_filters: bool,

    /// Opt-in Parquet writer tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-row-group-rows", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_row_group_rows: Option<usize>,

    /// Opt-in Parquet writer tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-page-size-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_page_size_bytes: Option<usize>,

    /// Opt-in Parquet writer tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-page-row-limit", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_page_row_limit: Option<usize>,

    /// Opt-in Parquet writer tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-write-batch-rows", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_write_batch_rows: Option<usize>,

    /// Opt-in execution tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-row-group-writers", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_row_group_writers: Option<usize>,

    /// Opt-in execution tuning; existing defaults remain unchanged.
    #[arg(long = "parquet-write-buffered-batches", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_write_buffered_batches: Option<usize>,

    /// Opt-in execution tuning; existing defaults remain unchanged.
    #[arg(long = "perfect-hash-key-range", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    perfect_hash_key_range: Option<usize>,

    /// Opt-in execution tuning; existing defaults remain unchanged.
    #[arg(long = "hash-join-probe-buffer-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    hash_join_probe_buffer_bytes: Option<usize>,

    /// Opt-in optimizer tuning; existing defaults remain unchanged.
    #[arg(long = "join-inlist-pushdown-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    join_inlist_pushdown_bytes: Option<usize>,

    /// Opt-in optimizer tuning; existing defaults remain unchanged.
    #[arg(long = "join-inlist-pushdown-values", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    join_inlist_pushdown_values: Option<usize>,

    /// Opt-in optimizer tuning; existing defaults remain unchanged.
    #[arg(long = "sort-in-place-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    sort_in_place_bytes: Option<usize>,

    /// Opt-in scan/order tuning; existing defaults remain unchanged.
    #[arg(long = "prefer-existing-sort")]
    prefer_existing_sort: bool,

    /// Compress spilled intermediate data when disk bandwidth is limited.
    #[arg(long = "spill-compression", value_name = "CODEC", value_parser = ["uncompressed", "lz4_frame", "zstd"])]
    spill_compression: Option<String>,

    /// Maximum queued RecordBatches per output file, at least two.
    #[arg(long = "max-buffered-output-batches", value_name = "COUNT", value_parser = parse_output_buffer_batches)]
    max_buffered_output_batches: Option<usize>,

    /// Override estimated fallback filter selectivity, from 0 to 100.
    #[arg(long = "default-filter-selectivity", value_name = "PERCENT", value_parser = parse_filter_selectivity)]
    default_filter_selectivity: Option<u8>,

    /// Tune Parquet writer statistics for selective queries.
    #[arg(long = "parquet-column-index-truncate", value_name = "LENGTH", value_parser = parse_positive_tuning_usize)]
    parquet_column_index_truncate: Option<usize>,

    /// Tune Parquet writer statistics for selective queries.
    #[arg(long = "parquet-statistics-truncate", value_name = "LENGTH", value_parser = parse_positive_tuning_usize)]
    parquet_statistics_truncate: Option<usize>,

    /// Opt-in Parquet writer tuning for analytical data.
    #[arg(long = "parquet-bloom-ndv", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_bloom_ndv: Option<usize>,

    /// Opt-in Parquet writer tuning for analytical data.
    #[arg(long = "parquet-dictionary-page-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_dictionary_page_bytes: Option<usize>,

    /// Optional performance tuning for Parquet-backed analytics.
    #[arg(long = "parquet-row-group-bytes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    parquet_row_group_bytes: Option<usize>,

    /// Optional performance tuning for Parquet-backed analytics.
    #[arg(long = "optimizer-passes", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    optimizer_passes: Option<usize>,

    /// Opt-in configuration for large analytical workloads.
    #[arg(long = "parquet-serial-file-write")]
    parquet_serial_file_write: bool,

    /// Tune min-parallel-output-files for large Parquet datasets.
    #[arg(long = "min-parallel-output-files", value_name = "VALUE", value_parser = parse_positive_tuning_usize)]
    min_parallel_output_files: Option<usize>,

    /// Retain UNION execution instead of planning an Interleave.
    #[arg(long = "prefer-existing-union")]
    prefer_existing_union: bool,

    #[arg(
        long = "output-parquet",
        value_name = "DIR",
        conflicts_with = "max_rows"
    )]
    output_parquet: Option<PathBuf>,

    #[arg(
        long = "output-csv",
        value_name = "DIR",
        conflicts_with_all = ["max_rows", "output_parquet"]
    )]
    output_csv: Option<PathBuf>,

    #[arg(
        long = "output-json",
        value_name = "DIR",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv"]
    )]
    output_json: Option<PathBuf>,

    #[arg(
        long = "output-arrow",
        value_name = "FILE",
        conflicts_with_all = ["max_rows", "output_parquet", "output_csv", "output_json"]
    )]
    output_arrow: Option<PathBuf>,

    /// Compress Arrow IPC file buffers. Default is uncompressed.
    #[arg(
        long = "arrow-compression",
        value_enum,
        value_name = "CODEC",
        requires = "output_arrow"
    )]
    arrow_compression: Option<ArrowCompression>,

    #[arg(long = "output-single-file")]
    output_single_file: bool,

    /// Soft row limit per file for partitioned CSV, JSON, and Parquet exports.
    #[arg(long = "output-rows-per-file", value_name = "ROWS", value_parser = parse_output_rows_per_file, conflicts_with = "output_single_file")]
    output_rows_per_file: Option<usize>,

    /// Select the codec for --output-parquet or --save-table.
    #[arg(long = "parquet-compression", value_name = "CODEC")]
    parquet_compression: Option<String>,
}

enum AutoInputKind {
    Csv,
    Json,
    Parquet,
}

fn classify_auto_input(
    path: &std::path::Path,
) -> Result<(AutoInputKind, String), Box<dyn std::error::Error>> {
    if path.is_dir() {
        return Err("--input needs a file; use --csv, --json or --parquet for directories".into());
    }
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .ok_or("--input needs a .csv, .json, .jsonl, .ndjson or .parquet extension")?;
    let kind = match extension.to_ascii_lowercase().as_str() {
        "csv" => AutoInputKind::Csv,
        "json" | "jsonl" | "ndjson" => AutoInputKind::Json,
        "parquet" => AutoInputKind::Parquet,
        _ => return Err(format!("Unsupported --input extension: .{extension}").into()),
    };
    Ok((kind, format!(".{extension}")))
}

fn resolve_auto_input(args: &mut Args) -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = args.auto_input.as_ref() else {
        return Ok(());
    };
    let (kind, extension) = classify_auto_input(path)?;
    match kind {
        AutoInputKind::Csv => {
            args.csv = Some(path.clone());
            args.csv_extension.get_or_insert(extension);
        }
        AutoInputKind::Json => {
            args.json = Some(path.clone());
            args.json_extension.get_or_insert(extension);
        }
        AutoInputKind::Parquet => {
            args.parquet = Some(path.clone());
            args.parquet_extension.get_or_insert(extension);
        }
    }
    Ok(())
}

fn parse_ascii_csv_char(value: &str) -> Option<u8> {
    match value.as_bytes() {
        [byte] if *byte != b'\n' && *byte != b'\r' => Some(*byte),
        _ => None,
    }
}

fn parse_delimiter(value: &str) -> Result<u8, String> {
    parse_ascii_csv_char(value)
        .ok_or_else(|| "CSV delimiter must be one ASCII character other than newline".into())
}

fn parse_csv_char(value: &str) -> Result<u8, String> {
    parse_ascii_csv_char(value)
        .ok_or_else(|| "CSV character must be one ASCII character other than newline".into())
}

fn parse_csv_null_value(value: &str) -> Result<String, String> {
    if value.is_empty() || value.contains('\n') || value.contains('\r') {
        Err("CSV null marker cannot be empty or contain a newline".into())
    } else {
        Ok(value.to_owned())
    }
}

fn parse_file_extension(value: &str) -> Result<String, String> {
    if value.len() > 1
        && value.starts_with('.')
        && value[1..].bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        Ok(value.to_owned())
    } else {
        Err("Extension must start with '.' and contain only ASCII letters or digits".into())
    }
}

fn valid_identifier(name: &str) -> bool {
    name.bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn parse_table_name(value: &str) -> Result<String, String> {
    if valid_identifier(value) {
        Ok(value.to_owned())
    } else {
        Err("--table must be a SQL identifier".into())
    }
}

fn parse_named_table(value: &str) -> Result<(&str, &str), String> {
    let (name, path) = value
        .split_once('=')
        .ok_or_else(|| "Named table must use NAME=PATH".to_owned())?;
    if !valid_identifier(name) || path.is_empty() {
        return Err("Named table needs a SQL identifier and non-empty path".into());
    }
    Ok((name, path))
}

fn parse_union_by_name(value: &str) -> Result<(&str, Vec<&str>), String> {
    let (name, source_names) = parse_named_table(value)
        .map_err(|_| "--union-by-name requires NAME=TABLE1,TABLE2,...".to_owned())?;
    let sources: Vec<_> = source_names.split(',').collect();
    if sources.len() < 2 || sources.iter().any(|source| !valid_identifier(source)) {
        return Err("--union-by-name requires at least two valid table names".into());
    }
    Ok((name, sources))
}

fn parse_named_view(value: &str) -> Result<(&str, &str), String> {
    let (name, sql) = parse_named_table(value)
        .map_err(|_| "--sql-view must use NAME=SQL with a valid SQL identifier".to_owned())?;
    if sql.trim().is_empty() {
        return Err("--sql-view SQL query cannot be empty".into());
    }
    Ok((name, sql))
}

fn parse_schema_data_type(kind: &str, source: &str) -> Result<DataType, String> {
    let data_type = match kind.to_ascii_lowercase().as_str() {
        "utf8" | "string" => DataType::Utf8,
        "int8" | "tinyint" => DataType::Int8,
        "int16" | "smallint" => DataType::Int16,
        "int32" | "int" | "integer" => DataType::Int32,
        "int64" | "bigint" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" | "real" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "bool" | "boolean" => DataType::Boolean,
        "date32" | "date" => DataType::Date32,
        _ => return Err(format!("Unsupported {source} type: {kind}")),
    };
    Ok(data_type)
}

fn parse_column_schema(value: &str, source: &str) -> Result<Schema, String> {
    let mut fields = Vec::new();
    let mut names = HashSet::new();
    for entry in value.split(',') {
        let (name, kind) = entry
            .split_once(':')
            .ok_or_else(|| format!("{source} schema needs NAME:TYPE entries"))?;
        if !valid_identifier(name) {
            return Err(format!(
                "{source} schema column names must be SQL identifiers"
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(format!("Duplicate {source} schema column: {name}"));
        }
        let data_type = parse_schema_data_type(kind, source)?;
        fields.push(Field::new(name, data_type, true));
    }
    Ok(Schema::new(fields))
}

fn parse_integer_param<T: std::str::FromStr>(
    raw: &str,
    message: &str,
    wrap: fn(Option<T>) -> ScalarValue,
) -> Result<ScalarValue, String> {
    raw.parse::<T>()
        .map(|value| wrap(Some(value)))
        .map_err(|_| message.to_owned())
}

fn parse_finite_float_param<T>(
    raw: &str,
    invalid_message: &str,
    non_finite_message: &str,
    wrap: fn(Option<T>) -> ScalarValue,
) -> Result<ScalarValue, String>
where
    T: std::str::FromStr + Copy + Into<f64>,
{
    let number = raw.parse::<T>().map_err(|_| invalid_message.to_owned())?;
    if !number.into().is_finite() {
        return Err(non_finite_message.to_owned());
    }
    Ok(wrap(Some(number)))
}

fn parse_boolean_param(raw: &str) -> Result<ScalarValue, String> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(ScalarValue::Boolean(Some(true)))
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(ScalarValue::Boolean(Some(false)))
    } else {
        Err("--param bool must be true or false".to_owned())
    }
}

fn parse_sql_param(value: &str) -> Result<ScalarValue, String> {
    let (kind, raw) = value
        .split_once(':')
        .ok_or_else(|| "--param must use TYPE:VALUE".to_owned())?;
    match kind.to_ascii_lowercase().as_str() {
        "int8" | "tinyint" => parse_integer_param::<i8>(
            raw,
            "--param int8 needs a signed 8-bit integer",
            ScalarValue::Int8,
        ),
        "int16" | "smallint" => parse_integer_param::<i16>(
            raw,
            "--param int16 needs a signed 16-bit integer",
            ScalarValue::Int16,
        ),
        "int32" | "int" | "integer" => parse_integer_param::<i32>(
            raw,
            "--param int32 needs a signed 32-bit integer",
            ScalarValue::Int32,
        ),
        "int64" | "bigint" => parse_integer_param::<i64>(
            raw,
            "--param int64 needs a signed 64-bit integer",
            ScalarValue::Int64,
        ),
        "uint8" => parse_integer_param::<u8>(
            raw,
            "--param uint8 needs an unsigned 8-bit integer",
            ScalarValue::UInt8,
        ),
        "uint16" => parse_integer_param::<u16>(
            raw,
            "--param uint16 needs an unsigned 16-bit integer",
            ScalarValue::UInt16,
        ),
        "uint32" => parse_integer_param::<u32>(
            raw,
            "--param uint32 needs an unsigned 32-bit integer",
            ScalarValue::UInt32,
        ),
        "uint64" => parse_integer_param::<u64>(
            raw,
            "--param uint64 needs an unsigned 64-bit integer",
            ScalarValue::UInt64,
        ),
        "float32" | "real" => parse_finite_float_param::<f32>(
            raw,
            "--param float32 needs a number",
            "--param float32 must be finite",
            ScalarValue::Float32,
        ),
        "float64" | "double" => parse_finite_float_param::<f64>(
            raw,
            "--param float64 needs a number",
            "--param float64 must be finite",
            ScalarValue::Float64,
        ),
        "bool" | "boolean" => parse_boolean_param(raw),
        "utf8" | "string" => Ok(ScalarValue::Utf8(Some(raw.to_owned()))),
        "null" if raw.is_empty() => Ok(ScalarValue::Null),
        _ => Err(
            "--param supports int8/16/32/64, uint8/16/32/64, float32/64, bool, utf8, or null:"
                .into(),
        ),
    }
}

fn parse_named_param(value: &str) -> Result<(String, ScalarValue), String> {
    let (name, raw) = value
        .split_once('=')
        .filter(|(name, _)| valid_identifier(name))
        .ok_or_else(|| {
            "--named-param requires NAME=TYPE:VALUE and a valid identifier".to_owned()
        })?;
    let value = parse_sql_param(raw).map_err(|error| error.replace("--param", "--named-param"))?;
    Ok((name.to_owned(), value))
}

fn parse_parquet_partitions(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "Parquet partition")
}

fn parse_csv_schema(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "CSV")
}

fn parse_json_schema(value: &str) -> Result<Schema, String> {
    parse_column_schema(value, "JSON")
}

fn parse_hash_join_inlist_max_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--hash-join-inlist-max-kb")?
        .checked_mul(1024)
        .ok_or_else(|| "--hash-join-inlist-max-kb is too large".to_owned())
}

fn parse_hash_join_inlist_max_values(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--hash-join-inlist-max-values")?
        .checked_mul(1)
        .ok_or_else(|| "--hash-join-inlist-max-values is too large".to_owned())
}

fn parse_hash_join_single_partition_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--hash-join-single-partition-mb")?
        .checked_mul(1048576)
        .ok_or_else(|| "--hash-join-single-partition-mb is too large".to_owned())
}

fn parse_hash_join_single_partition_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--hash-join-single-partition-rows")?
        .checked_mul(1)
        .ok_or_else(|| "--hash-join-single-partition-rows is too large".to_owned())
}

fn parse_perfect_hash_small_build_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--perfect-hash-small-build-rows")?
        .checked_mul(1)
        .ok_or_else(|| "--perfect-hash-small-build-rows is too large".to_owned())
}

fn parse_sort_read_ahead_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--sort-read-ahead-mb")?
        .checked_mul(1048576)
        .ok_or_else(|| "--sort-read-ahead-mb is too large".to_owned())
}

fn parse_planning_concurrency_override(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--planning-concurrency")?
        .checked_mul(1)
        .ok_or_else(|| "--planning-concurrency is too large".to_owned())
}

fn parse_partial_aggregate_probe_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--partial-aggregate-probe-rows")
}

fn parse_subset_repartition_threshold(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--subset-repartition-threshold")
}

fn parse_hash_join_probe_buffer_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--hash-join-probe-buffer-mb")?
        .checked_mul(1048576)
        .ok_or_else(|| "--hash-join-probe-buffer-mb is too large".to_owned())
}

fn parse_sort_in_place_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--sort-in-place-kb")?
        .checked_mul(1024)
        .ok_or_else(|| "--sort-in-place-kb is too large".to_owned())
}

fn parse_positive_usize(value: &str, option: &str) -> Result<usize, String> {
    let count = value
        .parse::<usize>()
        .map_err(|_| format!("{option} requires a positive integer"))?;
    if count == 0 {
        return Err(format!("{option} must be greater than zero"));
    }
    Ok(count)
}

fn parse_csv_schema_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--csv-schema-rows")
}

fn parse_json_schema_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--json-schema-rows")
}

mod read_options;
use read_options::{csv_read_options, json_read_options, parquet_read_options};

mod csv_input;
use csv_input::register_csv_with_null_value;

fn parse_query_timeout(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| "--query-timeout requires a positive integer in seconds".to_owned())?;
    if seconds == 0 {
        return Err("--query-timeout must be greater than zero".into());
    }
    Ok(seconds)
}

fn parse_memory_limit_mib(value: &str) -> Result<usize, String> {
    let mib = value
        .parse::<usize>()
        .map_err(|_| "--memory-limit-mb requires a positive integer".to_owned())?;
    mib.checked_mul(1024 * 1024)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| "--memory-limit-mb must fit in a positive number of bytes".to_owned())
}

fn parse_batch_size(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--batch-size")
}

fn parse_target_partitions(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--target-partitions")
}

fn parse_scan_split_min_kb(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--scan-split-min-kb")?
        .checked_mul(1024)
        .ok_or_else(|| "--scan-split-min-kb exceeds the supported range".to_owned())
}

fn parse_positive_metadata_concurrency(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--metadata-fetch-concurrency")
}

fn parse_output_buffer_batches(value: &str) -> Result<usize, String> {
    let count = parse_positive_usize(value, "--max-buffered-output-batches")?;
    if count < 2 {
        return Err("--max-buffered-output-batches must be at least 2".to_owned());
    }
    Ok(count)
}

fn parse_filter_selectivity(value: &str) -> Result<u8, String> {
    value
        .parse::<u8>()
        .ok()
        .filter(|percent| *percent <= 100)
        .ok_or_else(|| "--default-filter-selectivity must be between 0 and 100".to_owned())
}

fn parse_positive_tuning_usize(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "Performance tuning value")
}

fn parse_join_dynamic_inlist_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--join-dynamic-inlist-kb")?
        .checked_mul(1024)
        .ok_or_else(|| "--join-dynamic-inlist-kb exceeds the supported range".into())
}

fn parse_join_dynamic_inlist_distinct(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--join-dynamic-inlist-distinct")
}

fn parse_perfect_hash_small_range(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--perfect-hash-small-range")
}

fn parse_sort_spill_reserve_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--sort-spill-reserve-mb")?
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "--sort-spill-reserve-mb exceeds the supported range".into())
}

fn parse_sort_in_place_threshold_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--sort-in-place-max-mb")?
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "--sort-in-place-max-mb exceeds the supported range".into())
}

fn parse_sort_pushdown_buffer_bytes(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--sort-pushdown-buffer-mb")?
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "--sort-pushdown-buffer-mb exceeds the supported range".into())
}

fn parse_partial_agg_probe_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--partial-agg-probe-rows")
}

fn parse_partial_agg_skip_ratio(value: &str) -> Result<f64, String> {
    let ratio = value.parse::<f64>()
        .map_err(|_| "--partial-agg-skip-ratio requires a finite ratio between 0 and 1")?;
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err("--partial-agg-skip-ratio requires a finite ratio between 0 and 1".into())
    }
}

fn parse_subset_repartition_min_partitions(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--subset-repartition-min-partitions")
}

fn parse_output_rows_per_file(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--output-rows-per-file")
}

fn parse_positive_rows(value: &str) -> Result<usize, String> {
    parse_positive_usize(value, "--max-rows")
}

mod validation;
use validation::{validate_format_options, validate_inputs};

mod session;
use session::new_session;

mod parquet_file_list;
use parquet_file_list::load_parquet_file_list;

mod registration;
use registration::register_inputs;

mod persistence;
use persistence::register_database_tables;

mod query;
use query::execute_script;

mod interactive;
use interactive::run_interactive;

mod setup;
use setup::{cache_tables, read_query, register_derived_tables};

fn database_root(args: &Args) -> Result<&std::path::Path, Box<dyn std::error::Error>> {
    Ok(args
        .database_dir
        .as_deref()
        .ok_or("Missing database directory")?)
}

fn normalized_export_path(
    destination: &std::path::Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if destination.exists() {
        return Ok(destination.canonicalize()?);
    }
    let parent = destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let name = destination
        .file_name()
        .ok_or("Output path must name a file or directory")?;
    Ok(parent.canonicalize()?.join(name))
}

// The database catalog treats subdirectories as stored tables. Exporting a
// CSV/JSON/Parquet directory there can make the next startup fail to load it.
fn reject_database_export_collision(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let Some(root) = args.database_dir.as_deref() else {
        return Ok(());
    };
    let root = root.canonicalize()?;
    for destination in [
        args.output_arrow.as_ref(),
        args.output_parquet.as_ref(),
        args.output_csv.as_ref(),
        args.output_json.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        let normalized = normalized_export_path(destination)?;
        if normalized.starts_with(&root) {
            return Err("Output path must not be inside --database-dir".into());
        }
    }
    Ok(())
}

async fn with_query_timeout<F>(
    timeout_seconds: Option<u64>,
    work: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    if let Some(seconds) = timeout_seconds {
        match tokio::time::timeout(std::time::Duration::from_secs(seconds), work).await {
            Ok(result) => result,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("SQL query timed out after {seconds} seconds"),
            )
            .into()),
        }
    } else {
        work.await
    }
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut parsed = Args::parse();
    resolve_auto_input(&mut parsed)?;
    let args = Arc::new(parsed);
    let mut parameter_names = HashSet::new();
    for (name, _) in &args.named_params {
        if !parameter_names.insert(name.as_str()) {
            return Err(format!("Duplicate named parameter: {name}").into());
        }
    }
    if let (Some(offset), Some(max_rows)) = (args.offset, args.max_rows) {
        offset
            .checked_add(max_rows)
            .ok_or("--offset plus --max-rows exceeds the supported range")?;
    }
    validate_inputs(&args)?;
    // Serialize local catalog reads and writes across RustSpark processes.
    // Keep this file handle alive through interactive sessions and exports.
    let _database_lock = if let Some(root) = args.database_dir.as_deref() {
        Some(persistence::lock_database(root)?)
    } else {
        None
    };
    reject_database_export_collision(&args)?;
    let session = new_session(&args)?;
    validate_format_options(&args)?;
    register_inputs(&session, &args).await?;
    if let Some(directory) = args.database_dir.as_deref() {
        register_database_tables(&session, directory).await?;
    }
    if let Some(destination) = args.backup_database.as_deref() {
        let root = database_root(&args)?;
        return persistence::backup_database(root, destination);
    }
    if args.interactive {
        if let Some(table) = args.compact_table.as_deref() {
            let root = database_root(&args)?;
            persistence::compact_saved_table(&session, root, &table.to_ascii_lowercase()).await?;
        }
        return run_interactive(&session, &args).await;
    }
    with_query_timeout(
        args.query_timeout,
        execute_registered_query(&session, &args),
    )
    .await
}

async fn execute_registered_query(
    session: &RustSparkSession,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(table) = args.compact_table.as_deref() {
        let root = database_root(args)?;
        persistence::compact_saved_table(session, root, &table.to_ascii_lowercase()).await?;
    }
    register_derived_tables(session, args).await?;
    let query = read_query(args).await?;
    cache_tables(session, args).await?;
    execute_script(session, &query, args).await
}

mod output;
use output::{write_arrow_ipc, write_stdout};

#[cfg(test)]
mod auto_input_tests {
    use super::{AutoInputKind, classify_auto_input};
    use std::path::Path;

    #[test]
    fn recognizes_extensions_case_insensitively() {
        assert!(matches!(
            classify_auto_input(Path::new("events.CSV")),
            Ok((AutoInputKind::Csv, extension)) if extension == ".CSV"
        ));
        assert!(matches!(
            classify_auto_input(Path::new("events.ndjson")),
            Ok((AutoInputKind::Json, extension)) if extension == ".ndjson"
        ));
        assert!(matches!(
            classify_auto_input(Path::new("events.PARQUET")),
            Ok((AutoInputKind::Parquet, extension)) if extension == ".PARQUET"
        ));
        assert!(classify_auto_input(Path::new("events.txt")).is_err());
    }
}

#[cfg(test)]
mod boolean_parameter_tests {
    use super::{ScalarValue, parse_boolean_param};

    #[test]
    fn preserves_case_insensitive_boolean_values() {
        assert_eq!(
            parse_boolean_param("TRUE").unwrap(),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            parse_boolean_param("false").unwrap(),
            ScalarValue::Boolean(Some(false))
        );
        assert!(parse_boolean_param("1").is_err());
    }
}
