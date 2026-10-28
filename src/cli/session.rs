use std::{path::Path, sync::Arc};

use datafusion::{
    execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    prelude::SessionConfig,
};
use rustspark::RustSparkSession;

use super::Args;

fn build_session_config(args: &Args) -> SessionConfig {
    let mut config = SessionConfig::new();
    {
        let parquet = &mut config.options_mut().execution.parquet;
        parquet.pushdown_filters = args.parquet_pushdown_filters;
        parquet.reorder_filters = args.parquet_reorder_filters;
        parquet.binary_as_string = args.parquet_binary_as_string;
        if args.parquet_view_arrays {
            parquet.schema_force_view_types = true;
        }
        if args.parquet_force_filter_selections {
            parquet.force_filter_selections = true;
        }
        if let Some(bytes) = args.parquet_predicate_cache_mb_bytes {
            parquet.max_predicate_cache_size = Some(bytes);
        }
        if let Some(bytes) = args.parquet_metadata_hint_kb_bytes {
            parquet.metadata_size_hint = Some(bytes);
        }
        if let Some(size) = args.parquet_max_in_list_size {
            parquet.max_in_list_size = size;
        }
        if args.parquet_bloom_filters {
            parquet.bloom_filter_on_read = true;
        }
    }
    if args.distinct_soft_limit {
        config.options_mut().optimizer.enable_distinct_aggregation_soft_limit = true;
    }
    if let Some(value) = args.hash_join_inlist_max_bytes {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_size = value;
    }
    if let Some(value) = args.hash_join_inlist_max_values {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_distinct_values = value;
    }
    if let Some(value) = args.hash_join_single_partition_bytes {
        config.options_mut().optimizer.hash_join_single_partition_threshold = value;
    }
    if let Some(value) = args.hash_join_single_partition_rows {
        config.options_mut().optimizer.hash_join_single_partition_threshold_rows = value;
    }
    if let Some(value) = args.perfect_hash_small_build_rows {
        config.options_mut().execution.perfect_hash_join_small_build_threshold = value;
    }
    if let Some(value) = args.sort_read_ahead_bytes {
        config.options_mut().execution.sort_pushdown_buffer_capacity = value;
    }
    if let Some(value) = args.planning_concurrency_override {
        config.options_mut().execution.planning_concurrency = value;
    }
    if let Some(value) = args.partial_aggregate_probe_rows {
        config.options_mut().execution.skip_partial_aggregation_probe_rows_threshold = value;
    }
    if let Some(value) = args.subset_repartition_threshold {
        config.options_mut().optimizer.subset_repartition_threshold = value;
    }
    if args.use_statistics_registry {
        config.options_mut().optimizer.use_statistics_registry = true;
    }
    if args.leaf_expression_pushdown {
        config.options_mut().optimizer.enable_leaf_expression_pushdown = true;
    }
    if args.union_filter_rewrite {
        config.options_mut().optimizer.enable_unions_to_filter = true;
    }
    if let Some(value) = args.hash_join_probe_buffer_mb {
        config.options_mut().execution.hash_join_buffering_capacity = value;
    }
    if let Some(value) = args.sort_in_place_kb {
        config.options_mut().execution.sort_in_place_threshold_bytes = value;
    }
    if let Some(bytes) = args.join_dynamic_inlist_bytes {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_size = bytes;
    }
    if let Some(count) = args.join_dynamic_inlist_distinct {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_distinct_values = count;
    }
    if let Some(count) = args.perfect_hash_small_range {
        config.options_mut().execution.perfect_hash_join_small_build_threshold = count;
    }
    if let Some(bytes) = args.sort_spill_reserve_bytes {
        config.options_mut().execution.sort_spill_reservation_bytes = bytes;
    }
    if let Some(bytes) = args.sort_in_place_threshold_bytes {
        config.options_mut().execution.sort_in_place_threshold_bytes = bytes;
    }
    if let Some(bytes) = args.sort_pushdown_buffer_bytes {
        config.options_mut().execution.sort_pushdown_buffer_capacity = bytes;
    }
    if let Some(value) = args.partial_agg_probe_rows {
        config.options_mut().execution.skip_partial_aggregation_probe_rows_threshold = value;
    }
    if let Some(value) = args.partial_agg_skip_ratio {
        config.options_mut().execution.skip_partial_aggregation_probe_ratio_threshold = value;
    }
    if let Some(value) = args.subset_repartition_min_partitions {
        config.options_mut().optimizer.subset_repartition_threshold = value;
    }
    if let Some(batch_size) = args.batch_size {
        config = config.with_batch_size(batch_size);
    }
    if args.filter_null_join_keys {
        config.options_mut().optimizer.filter_null_join_keys = true;
    }
    if let Some(partitions) = args.target_partitions {
        config = config.with_target_partitions(partitions);
    }
    if let Some(min_bytes) = args.scan_split_min_bytes {
        config = config
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(min_bytes);
    }
    if args.row_count_partition_estimates {
        config.options_mut().execution.use_row_number_estimates_to_optimize_partitioning = true;
    }
    if let Some(rows) = args.output_rows_per_file {
        config = config.set_usize("datafusion.execution.soft_max_rows_per_output_file", rows);
    }
    if args.piecewise_merge_join {
        config.options_mut().optimizer.enable_piecewise_merge_join = true;
    }
    if let Some(files) = args.metadata_fetch_concurrency {
        config = config.set_usize("datafusion.execution.meta_fetch_concurrency", files);
    }
    if let Some(limit) = args.parquet_in_list_pruning_limit {
        config.options_mut().execution.parquet.max_in_list_size = limit;
    }
    if let Some(value) = args.parquet_predicate_cache_bytes {
        config.options_mut().execution.parquet.max_predicate_cache_size = Some(value);
    }
    if let Some(value) = args.parquet_metadata_hint_bytes {
        config.options_mut().execution.parquet.metadata_size_hint = Some(value);
    }
    if args.parquet_force_row_selection {
        config.options_mut().execution.parquet.force_filter_selections = true;
    }
    if args.parquet_write_bloom_filters {
        config.options_mut().execution.parquet.bloom_filter_on_write = true;
    }
    if let Some(value) = args.parquet_row_group_rows {
        config.options_mut().execution.parquet.max_row_group_size = value;
    }
    if let Some(value) = args.parquet_page_size_bytes {
        config.options_mut().execution.parquet.data_pagesize_limit = value;
    }
    if let Some(value) = args.parquet_page_row_limit {
        config.options_mut().execution.parquet.data_page_row_count_limit = value;
    }
    if let Some(value) = args.parquet_write_batch_rows {
        config.options_mut().execution.parquet.write_batch_size = value;
    }
    if let Some(value) = args.parquet_row_group_writers {
        config.options_mut().execution.parquet.maximum_parallel_row_group_writers = value;
    }
    if let Some(value) = args.parquet_write_buffered_batches {
        config.options_mut().execution.parquet.maximum_buffered_record_batches_per_stream = value;
    }
    if let Some(value) = args.perfect_hash_key_range {
        config.options_mut().execution.perfect_hash_join_small_build_threshold = value;
    }
    if let Some(value) = args.hash_join_probe_buffer_bytes {
        config.options_mut().execution.hash_join_buffering_capacity = value;
    }
    if let Some(value) = args.join_inlist_pushdown_bytes {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_size = value;
    }
    if let Some(value) = args.join_inlist_pushdown_values {
        config.options_mut().optimizer.hash_join_inlist_pushdown_max_distinct_values = value;
    }
    if let Some(value) = args.sort_in_place_bytes {
        config.options_mut().execution.sort_in_place_threshold_bytes = value;
    }
    if args.window_topn {
        config.options_mut().optimizer.enable_window_topn = true;
    }
    if args.prefer_existing_sort {
        config.options_mut().optimizer.prefer_existing_sort = true;
    }
    if args.split_file_groups_by_statistics {
        config.options_mut().execution.split_file_groups_by_statistics = true;
    }
    if let Some(probability) = args.parquet_bloom_fpp {
        config.options_mut().execution.parquet.bloom_filter_fpp = Some(probability);
    }
    if args.disable_batch_coalescing {
        config.options_mut().execution.coalesce_batches = false;
    }
    if let Some(value) = args.max_spill_file_bytes {
        config = config.set_usize("datafusion.execution.max_spill_file_size_bytes", value);
    }
    if args.prefer_sort_merge_join {
        config.options_mut().optimizer.prefer_hash_join = false;
    }
    if let Some(codec) = args.spill_compression.as_deref() {
        config = config.set_str("datafusion.execution.spill_compression", codec);
    }
    if let Some(value) = args.max_buffered_output_batches {
        config = config.set_usize("datafusion.execution.max_buffered_batches_per_output_file", value);
    }
    if let Some(value) = args.default_filter_selectivity {
        config.options_mut().optimizer.default_filter_selectivity = value;
    }
    if let Some(value) = args.parquet_column_index_truncate {
        config = config.set_usize("datafusion.execution.parquet.column_index_truncate_length", value);
    }
    if let Some(value) = args.parquet_statistics_truncate {
        config = config.set_usize("datafusion.execution.parquet.statistics_truncate_length", value);
    }
    if let Some(value) = args.parquet_bloom_ndv {
        config = config.set_usize("datafusion.execution.parquet.bloom_filter_ndv", value);
    }
    if let Some(value) = args.parquet_dictionary_page_bytes {
        config = config.set_usize("datafusion.execution.parquet.dictionary_page_size_limit", value);
    }
    if let Some(value) = args.parquet_row_group_bytes {
        config = config.set_usize("datafusion.execution.parquet.max_row_group_bytes", value);
    }
    if let Some(value) = args.optimizer_passes {
        config = config.set_usize("datafusion.optimizer.max_passes", value);
    }
    if args.parquet_serial_file_write {
        config.options_mut().execution.parquet.allow_single_file_parallelism = false;
    }
    if let Some(value) = args.min_parallel_output_files {
        config = config.set_usize("datafusion.execution.minimum_parallel_output_files", value);
    }
    if args.disable_file_work_stealing {
        config.options_mut().execution.enable_file_stream_work_stealing = false;
    }
    if args.enforce_join_batch_size {
        config.options_mut().execution.enforce_batch_size_in_joins = true;
    }
    if args.skip_table_statistics {
        config.options_mut().execution.collect_statistics = false;
    }
    if let Some(count) = args.preserve_file_partitions {
        config = config.set_usize("datafusion.optimizer.preserve_file_partitions", count);
    }
    if args.prefer_existing_union {
        config.options_mut().optimizer.prefer_existing_union = true;
    }
    config
}

fn validate_spill_directory(
    path: &Path,
    database_root: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_dir() {
        return Err(format!(
            "--spill-dir must name an existing directory: {}",
            path.display()
        )
        .into());
    }
    if let Some(root) = database_root
        && path.canonicalize()?.starts_with(root.canonicalize()?)
    {
        return Err("--spill-dir must not be inside --database-dir".into());
    }
    Ok(())
}

fn custom_runtime(args: &Args) -> Result<Option<Arc<RuntimeEnv>>, Box<dyn std::error::Error>> {
    if args.memory_limit_bytes.is_none() && args.spill_dir.is_none() {
        return Ok(None);
    }
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(limit) = args.memory_limit_bytes {
        runtime = runtime.with_memory_limit(limit, 1.0);
    }
    if let Some(path) = args.spill_dir.as_deref() {
        validate_spill_directory(path, args.database_dir.as_deref())?;
        runtime = runtime.with_temp_file_path(path.to_path_buf());
    }
    Ok(Some(runtime.build_arc()?))
}

pub(super) fn new_session(
    args: &Args,
) -> Result<Arc<RustSparkSession>, Box<dyn std::error::Error>> {
    let config = build_session_config(args);
    let session = match custom_runtime(args)? {
        Some(runtime) => RustSparkSession::with_config_and_runtime(config, runtime),
        None => RustSparkSession::with_config(config),
    };
    Ok(Arc::new(session))
}

#[cfg(test)]
mod tests {
    use super::{Args, custom_runtime, validate_spill_directory};
    use clap::Parser;
    use std::fs;

    #[test]
    fn allows_tuning_parquet_in_list_pruning_without_changing_the_default() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--parquet-in-list-pruning-limit", "128",
        ]).unwrap();
        let default_limit = datafusion::prelude::SessionConfig::new()
            .options().execution.parquet.max_in_list_size;
        assert_eq!(super::build_session_config(&original)
            .options().execution.parquet.max_in_list_size, default_limit);
        assert_eq!(super::build_session_config(&tuned)
            .options().execution.parquet.max_in_list_size, 128);
        assert!(Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--parquet-in-list-pruning-limit", "0",
        ]).is_err());
    }

    #[test]
    fn sets_parquet_predicate_cache_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-predicate-cache-bytes", "67108864"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.max_predicate_cache_size, Some(67108864));
    }

    #[test]
    fn sets_parquet_metadata_hint_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-metadata-hint-bytes", "1048576"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.metadata_size_hint, Some(1048576));
    }

    #[test]
    fn sets_parquet_force_row_selection_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-pushdown-filters", "--parquet-force-row-selection"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.force_filter_selections, true);
    }

    #[test]
    fn sets_parquet_write_bloom_filters_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-write-bloom-filters"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.bloom_filter_on_write, true);
    }

    #[test]
    fn sets_parquet_row_group_rows_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-row-group-rows", "131072"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.max_row_group_size, 131072);
    }

    #[test]
    fn sets_parquet_page_size_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-page-size-bytes", "65536"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.data_pagesize_limit, 65536);
    }

    #[test]
    fn sets_parquet_page_row_limit_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-page-row-limit", "4096"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.data_page_row_count_limit, 4096);
    }

    #[test]
    fn sets_parquet_write_batch_rows_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-write-batch-rows", "16384"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.write_batch_size, 16384);
    }

    #[test]
    fn sets_parquet_row_group_writers_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-row-group-writers", "4"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.maximum_parallel_row_group_writers, 4);
    }

    #[test]
    fn sets_parquet_write_buffered_batches_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--parquet-write-buffered-batches", "8"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.parquet.maximum_buffered_record_batches_per_stream, 8);
    }

    #[test]
    fn sets_perfect_hash_key_range_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--perfect-hash-key-range", "65536"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.perfect_hash_join_small_build_threshold, 65536);
    }

    #[test]
    fn sets_hash_join_probe_buffer_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--hash-join-probe-buffer-bytes", "4194304"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.hash_join_buffering_capacity, 4194304);
    }

    #[test]
    fn sets_join_inlist_pushdown_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--join-inlist-pushdown-bytes", "262144"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().optimizer.hash_join_inlist_pushdown_max_size, 262144);
    }

    #[test]
    fn sets_join_inlist_pushdown_values_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--join-inlist-pushdown-values", "256"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().optimizer.hash_join_inlist_pushdown_max_distinct_values, 256);
    }

    #[test]
    fn sets_sort_in_place_bytes_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--sort-in-place-bytes", "8388608"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().execution.sort_in_place_threshold_bytes, 8388608);
    }

    #[test]
    fn sets_window_topn_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--window-topn"]).unwrap();
        let config = super::build_session_config(&args);
        assert_eq!(config.options().optimizer.enable_window_topn, true);
    }

    #[test]
    fn sets_prefer_existing_sort_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--prefer-existing-sort"]).unwrap();
        let config = super::build_session_config(&args);
        assert!(config.options().optimizer.prefer_existing_sort);
    }

    #[test]
    fn sets_split_file_groups_by_statistics_when_requested() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--split-file-groups-by-statistics"]).unwrap();
        let config = super::build_session_config(&args);
        assert!(config.options().execution.split_file_groups_by_statistics);
    }

    #[test]
    fn validates_and_configures_parquet_bloom_fpp() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-bloom-fpp",
            "0.01",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.bloom_filter_fpp,
            defaults.options().execution.parquet.bloom_filter_fpp
        );
        assert_eq!(
            configured.options().execution.parquet.bloom_filter_fpp,
            Some(0.01)
        );
        for invalid in ["0", "1", "NaN", "inf", "-0.1", "nope"] {
            assert!(
                Args::try_parse_from([
                    "rustspark",
                    "--sql",
                    "SELECT 1",
                    "--parquet-bloom-fpp",
                    invalid,
                ])
                .is_err()
            );
        }
    }

    #[test]
    fn toggles_disable_batch_coalescing_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--disable-batch-coalescing",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(baseline.options().execution.coalesce_batches, defaults.options().execution.coalesce_batches);
        assert_eq!(configured.options().execution.coalesce_batches, false);
    }

    #[test]
    fn configures_max_spill_file_bytes_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--max-spill-file-bytes",
            "268435456",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.max_spill_file_size_bytes.get(),
            defaults.options().execution.max_spill_file_size_bytes.get()
        );
        assert_eq!(configured.options().execution.max_spill_file_size_bytes.get(), 268435456);
    }

    #[test]
    fn toggles_prefer_sort_merge_join_only_when_requested() {
        let default_args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned_args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--prefer-sort-merge-join",
        ])
        .unwrap();
        let baseline = super::build_session_config(&default_args);
        let configured = super::build_session_config(&tuned_args);
        let datafusion_default = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().optimizer.prefer_hash_join,
            datafusion_default.options().optimizer.prefer_hash_join
        );
        assert_eq!(configured.options().optimizer.prefer_hash_join, false);
    }

    #[test]
    fn supports_opt_in_spill_compression_codecs() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--spill-compression",
            "lz4_frame",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.spill_compression.to_string(),
            defaults.options().execution.spill_compression.to_string()
        );
        assert_eq!(
            configured.options().execution.spill_compression.to_string(),
            "lz4_frame"
        );
        assert!(Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--spill-compression",
            "not-a-codec",
        ])
        .is_err());
    }

    #[test]
    fn validates_and_configures_max_buffered_output_batches() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--max-buffered-output-batches",
            "8",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(baseline.options().execution.max_buffered_batches_per_output_file.get(), defaults.options().execution.max_buffered_batches_per_output_file.get());
        assert_eq!(configured.options().execution.max_buffered_batches_per_output_file.get(), 8);
        assert!(Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--max-buffered-output-batches",
            "1",
        ])
        .is_err());
    }

    #[test]
    fn validates_and_configures_default_filter_selectivity() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--default-filter-selectivity",
            "10",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(baseline.options().optimizer.default_filter_selectivity, defaults.options().optimizer.default_filter_selectivity);
        assert_eq!(configured.options().optimizer.default_filter_selectivity, 10);
        assert!(Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--default-filter-selectivity",
            "101",
        ])
        .is_err());
    }

    #[test]
    fn configures_parquet_column_index_truncate_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-column-index-truncate",
            "256",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.column_index_truncate_length,
            defaults.options().execution.parquet.column_index_truncate_length
        );
        assert_eq!(configured.options().execution.parquet.column_index_truncate_length, Some(256));
    }

    #[test]
    fn configures_parquet_statistics_truncate_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-statistics-truncate",
            "256",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.statistics_truncate_length,
            defaults.options().execution.parquet.statistics_truncate_length
        );
        assert_eq!(configured.options().execution.parquet.statistics_truncate_length, Some(256));
    }

    #[test]
    fn configures_parquet_bloom_ndv_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-bloom-ndv",
            "10000",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.bloom_filter_ndv,
            defaults.options().execution.parquet.bloom_filter_ndv
        );
        assert_eq!(configured.options().execution.parquet.bloom_filter_ndv, Some(10000));
    }

    #[test]
    fn configures_parquet_dictionary_page_bytes_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-dictionary-page-bytes",
            "2097152",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.dictionary_page_size_limit,
            defaults.options().execution.parquet.dictionary_page_size_limit
        );
        assert_eq!(configured.options().execution.parquet.dictionary_page_size_limit, 2097152);
    }

    #[test]
    fn configures_parquet_row_group_bytes_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-row-group-bytes",
            "134217728",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.parquet.max_row_group_bytes.map(|v| v.get()),
            defaults.options().execution.parquet.max_row_group_bytes.map(|v| v.get())
        );
        assert_eq!(
            configured.options().execution.parquet.max_row_group_bytes.map(|v| v.get()),
            Some(134217728)
        );
    }

    #[test]
    fn configures_optimizer_passes_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--optimizer-passes",
            "5",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().optimizer.max_passes,
            defaults.options().optimizer.max_passes
        );
        assert_eq!(
            configured.options().optimizer.max_passes,
            5
        );
    }

    #[test]
    fn toggles_parquet_serial_file_write_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--parquet-serial-file-write",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(baseline.options().execution.parquet.allow_single_file_parallelism, defaults.options().execution.parquet.allow_single_file_parallelism);
        assert_eq!(configured.options().execution.parquet.allow_single_file_parallelism, false);
    }

    #[test]
    fn configures_min_parallel_output_files_without_changing_defaults() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--min-parallel-output-files",
            "8",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.minimum_parallel_output_files.get(),
            defaults.options().execution.minimum_parallel_output_files.get()
        );
        assert_eq!(configured.options().execution.minimum_parallel_output_files.get(), 8);
    }

    #[test]
    fn toggles_disable_file_work_stealing_only_when_requested() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--disable-file-work-stealing",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.enable_file_stream_work_stealing,
            defaults.options().execution.enable_file_stream_work_stealing
        );
        assert_eq!(configured.options().execution.enable_file_stream_work_stealing, false);
    }

    #[test]
    fn toggles_enforce_join_batch_size_only_when_requested() {
        let default_args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned_args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--enforce-join-batch-size",
        ])
        .unwrap();
        let baseline = super::build_session_config(&default_args);
        let configured = super::build_session_config(&tuned_args);
        let datafusion_default = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.enforce_batch_size_in_joins,
            datafusion_default.options().execution.enforce_batch_size_in_joins
        );
        assert_eq!(configured.options().execution.enforce_batch_size_in_joins, true);
    }

    #[test]
    fn toggles_skip_table_statistics_only_when_requested() {
        let default_args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned_args = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--skip-table-statistics",
        ])
        .unwrap();
        let baseline = super::build_session_config(&default_args);
        let configured = super::build_session_config(&tuned_args);
        let datafusion_default = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().execution.collect_statistics,
            datafusion_default.options().execution.collect_statistics
        );
        assert_eq!(configured.options().execution.collect_statistics, false);
    }

    #[test]
    fn preserves_hive_file_partitions_when_requested() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--preserve-file-partitions",
            "365",
        ])
        .unwrap();
        let base_config = super::build_session_config(&original);
        let tuned_config = super::build_session_config(&tuned);
        let default_config = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            base_config.options().optimizer.preserve_file_partitions,
            default_config.options().optimizer.preserve_file_partitions
        );
        assert_eq!(tuned_config.options().optimizer.preserve_file_partitions, 365);
    }

    #[test]
    fn toggles_prefer_existing_union_only_when_requested() {
        let original = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        let tuned = Args::try_parse_from([
            "rustspark",
            "--sql",
            "SELECT 1",
            "--prefer-existing-union",
        ])
        .unwrap();
        let baseline = super::build_session_config(&original);
        let configured = super::build_session_config(&tuned);
        let defaults = datafusion::prelude::SessionConfig::new();
        assert_eq!(
            baseline.options().optimizer.prefer_existing_union,
            defaults.options().optimizer.prefer_existing_union
        );
        assert_eq!(configured.options().optimizer.prefer_existing_union, true);
    }

    #[test]
    fn can_enable_parquet_bloom_filter_pruning() {
        let args = Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--parquet-bloom-filters",
        ]).unwrap();
        let config = super::build_session_config(&args);
        assert!(config.options().execution.parquet.bloom_filter_on_read);
    }

    #[test]
    fn configures_parquet_in_list_pruning() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--parquet-max-in-list-size", "64"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.parquet.max_in_list_size, 64);
    }

    #[test]
    fn configures_parquet_metadata_prefetch_hint() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--parquet-metadata-hint-kb", "512"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.parquet.metadata_size_hint,
            Some(512 * 1024));
    }

    #[test]
    fn configures_parquet_predicate_cache() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--parquet-pushdown-filters", "--parquet-predicate-cache-mb", "32"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.parquet.max_predicate_cache_size,
            Some(32 * 1024 * 1024));
    }

    #[test]
    fn configures_parquet_filter_selections() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--parquet-pushdown-filters", "--parquet-force-filter-selections"]).unwrap();
        assert!(super::build_session_config(&args).options().execution.parquet.force_filter_selections);
    }

    #[test]
    fn configures_parquet_view_arrays() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--parquet-view-arrays"]).unwrap();
        assert!(super::build_session_config(&args).options().execution.parquet.schema_force_view_types);
    }

    #[test]
    fn configures_hash_join_probe_buffer_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--hash-join-probe-buffer-mb", "32"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.hash_join_buffering_capacity, 32 * 1024 * 1024);
    }

    #[test]
    fn configures_hash_join_single_partition_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--hash-join-single-partition-mb", "64"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().optimizer.hash_join_single_partition_threshold, 64 * 1024 * 1024);
    }

    #[test]
    fn configures_join_dynamic_inlist_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--join-dynamic-inlist-kb", "256"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().optimizer.hash_join_inlist_pushdown_max_size, 256 * 1024);
    }

    #[test]
    fn configures_join_dynamic_inlist_distinct() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--join-dynamic-inlist-distinct", "512"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().optimizer.hash_join_inlist_pushdown_max_distinct_values, 512);
    }

    #[test]
    fn configures_perfect_hash_small_range() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--perfect-hash-small-range", "16384"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.perfect_hash_join_small_build_threshold, 16384);
    }

    #[test]
    fn configures_hash_join_single_partition_rows() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--hash-join-single-partition-rows", "1000000"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().optimizer.hash_join_single_partition_threshold_rows, 1000000);
    }

    #[test]
    fn configures_sort_spill_reserve_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--sort-spill-reserve-mb", "64"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.sort_spill_reservation_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn configures_sort_in_place_threshold_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--sort-in-place-max-mb", "16"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.sort_in_place_threshold_bytes, 16 * 1024 * 1024);
    }

    #[test]
    fn configures_sort_pushdown_buffer_bytes() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--sort-pushdown-buffer-mb", "64"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.sort_pushdown_buffer_capacity, 64 * 1024 * 1024);
    }

    #[test]
    fn configures_partial_agg_probe_rows() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--partial-agg-probe-rows", "50000"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.skip_partial_aggregation_probe_rows_threshold, 50000);
    }

    #[test]
    fn configures_partial_agg_skip_ratio() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--partial-agg-skip-ratio", "0.85"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().execution.skip_partial_aggregation_probe_ratio_threshold, 0.85);
    }

    #[test]
    fn configures_subset_repartition_min_partitions() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1",
            "--subset-repartition-min-partitions", "8"]).unwrap();
        assert_eq!(super::build_session_config(&args).options().optimizer.subset_repartition_threshold, 8);
    }

    #[test]
    fn leaves_the_default_session_without_custom_runtime() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1"]).unwrap();
        assert!(custom_runtime(&args).unwrap().is_none());
    }

    #[test]
    fn validates_spill_paths_without_changing_runtime_setup() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("db");
        let nested = database.join("spill");
        let separate = directory.path().join("spill");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir(&separate).unwrap();

        assert!(validate_spill_directory(&separate, Some(&database)).is_ok());
        assert!(validate_spill_directory(&nested, Some(&database)).is_err());
        assert!(validate_spill_directory(&database, Some(&database)).is_err());
        assert!(validate_spill_directory(&directory.path().join("missing"), None).is_err());
    }
}

#[cfg(test)]
mod perf_hash_join_inlist_size {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn config_uses_requested_value() {
        let args = Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-inlist-max-kb", "8",
        ])
        .unwrap();
        let config = build_session_config(&args);
        assert_eq!(config.options().optimizer.hash_join_inlist_pushdown_max_size, 8192);
        assert!(Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-inlist-max-kb", "0",
        ])
        .is_err());
    }
}

#[cfg(test)]
mod perf_hash_join_inlist_values {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn config_uses_requested_value() {
        let args = Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-inlist-max-values", "8",
        ])
        .unwrap();
        let config = build_session_config(&args);
        assert_eq!(config.options().optimizer.hash_join_inlist_pushdown_max_distinct_values, 8);
        assert!(Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-inlist-max-values", "0",
        ])
        .is_err());
    }
}

#[cfg(test)]
mod perf_hash_join_single_partition_bytes {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn reads_nonzero_tuning_value() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--hash-join-single-partition-mb", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().optimizer.hash_join_single_partition_threshold, 8388608);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--hash-join-single-partition-mb", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_hash_join_single_partition_rows {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn reads_nonzero_tuning_value() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--hash-join-single-partition-rows", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().optimizer.hash_join_single_partition_threshold_rows, 8);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--hash-join-single-partition-rows", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_perfect_hash_small_build {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn reads_nonzero_tuning_value() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--perfect-hash-small-build-rows", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().execution.perfect_hash_join_small_build_threshold, 8);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--perfect-hash-small-build-rows", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_sort_read_ahead {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn validates_and_applies_tuning() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--sort-read-ahead-mb", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().execution.sort_pushdown_buffer_capacity, 8388608);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--sort-read-ahead-mb", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_parallel_planning {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn validates_and_applies_tuning() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--planning-concurrency", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().execution.planning_concurrency, 8);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--planning-concurrency", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_partial_aggregate_probe {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn cli_flag_updates_optimizer_config() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--partial-aggregate-probe-rows", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().execution.skip_partial_aggregation_probe_rows_threshold, 8);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--partial-aggregate-probe-rows", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_subset_repartition {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn cli_flag_updates_optimizer_config() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--subset-repartition-threshold", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().optimizer.subset_repartition_threshold, 8);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--subset-repartition-threshold", "0"]).is_err());
    }
}

#[cfg(test)]
mod perf_statistics_registry {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn cli_flag_updates_optimizer_config() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--use-statistics-registry"]).unwrap();
        assert_eq!(build_session_config(&args).options().optimizer.use_statistics_registry, true);
    }
}

#[cfg(test)]
mod perf_leaf_expression_pushdown {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn cli_enables_optimizer_flag() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--leaf-expression-pushdown"]).unwrap();
        assert!(build_session_config(&args).options().optimizer.enable_leaf_expression_pushdown);
    }
}

#[cfg(test)]
mod perf_union_filter_rewrite {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn cli_enables_optimizer_flag() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--union-filter-rewrite"]).unwrap();
        assert!(build_session_config(&args).options().optimizer.enable_unions_to_filter);
    }
}

#[cfg(test)]
mod perf_hash_join_probe_buffer {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn config_uses_requested_value() {
        let args = Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-probe-buffer-mb", "8",
        ])
        .unwrap();
        let config = build_session_config(&args);
        assert_eq!(config.options().execution.hash_join_buffering_capacity, 8388608);
        assert!(Args::try_parse_from([
            "rustspark", "--sql", "SELECT 1", "--hash-join-probe-buffer-mb", "0",
        ])
        .is_err());
    }
}

#[cfg(test)]
mod perf_sort_in_place {
    use super::{Args, build_session_config};
    use clap::Parser;

    #[test]
    fn validates_and_applies_tuning() {
        let args = Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--sort-in-place-kb", "8"]).unwrap();
        assert_eq!(build_session_config(&args).options().execution.sort_in_place_threshold_bytes, 8192);
        assert!(Args::try_parse_from(["rustspark", "--sql", "SELECT 1", "--sort-in-place-kb", "0"]).is_err());
    }
}
