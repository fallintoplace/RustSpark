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
        if let Some(bytes) = args.parquet_predicate_cache_bytes {
            parquet.max_predicate_cache_size = Some(bytes);
        }
        if let Some(bytes) = args.parquet_metadata_hint_bytes {
            parquet.metadata_size_hint = Some(bytes);
        }
        if let Some(size) = args.parquet_max_in_list_size {
            parquet.max_in_list_size = size;
        }
        if args.parquet_bloom_filters {
            parquet.bloom_filter_on_read = true;
        }
    }
    if let Some(bytes) = args.hash_join_probe_buffer_bytes {
        config.options_mut().execution.hash_join_buffering_capacity = bytes;
    }
    if let Some(bytes) = args.hash_join_single_partition_bytes {
        config.options_mut().optimizer.hash_join_single_partition_threshold = bytes;
    }
    if let Some(batch_size) = args.batch_size {
        config = config.with_batch_size(batch_size);
    }
    if let Some(partitions) = args.target_partitions {
        config = config.with_target_partitions(partitions);
    }
    if let Some(min_bytes) = args.scan_split_min_bytes {
        config = config
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(min_bytes);
    }
    if let Some(rows) = args.output_rows_per_file {
        config = config.set_usize("datafusion.execution.soft_max_rows_per_output_file", rows);
    }
    if let Some(files) = args.metadata_fetch_concurrency {
        config = config.set_usize("datafusion.execution.meta_fetch_concurrency", files);
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
