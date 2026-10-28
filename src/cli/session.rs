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
    }
    if args.distinct_soft_limit {
        config.options_mut().optimizer.enable_distinct_aggregation_soft_limit = true;
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
    if let Some(value) = args.parquet_row_group_bytes {
        config = config.set_usize("datafusion.execution.parquet.max_row_group_bytes", value);
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
