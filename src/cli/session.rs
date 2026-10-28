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
