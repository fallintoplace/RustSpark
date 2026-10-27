use std::sync::Arc;

use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionConfig};
use rustspark::RustSparkSession;

use super::Args;

fn build_session_config(args: &Args) -> SessionConfig {
    let mut config = SessionConfig::new();
    {
        let parquet = &mut config.options_mut().execution.parquet;
        parquet.pushdown_filters = args.parquet_pushdown_filters;
        parquet.reorder_filters = args.parquet_reorder_filters;
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
    if let Some(files) = args.metadata_fetch_concurrency {
        config = config.set_usize("datafusion.execution.meta_fetch_concurrency", files);
    }
    config
}

pub(super) fn new_session(
    args: &Args,
) -> Result<Arc<RustSparkSession>, Box<dyn std::error::Error>> {
    let config = build_session_config(args);
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(limit) = args.memory_limit_bytes {
        runtime = runtime.with_memory_limit(limit, 1.0);
    }
    if let Some(path) = args.spill_dir.as_deref() {
        if !path.is_dir() {
            return Err(format!(
                "--spill-dir must name an existing directory: {}",
                path.display()
            )
            .into());
        }
        if let Some(root) = args.database_dir.as_deref()
            && path.canonicalize()?.starts_with(root.canonicalize()?)
        {
            return Err("--spill-dir must not be inside --database-dir".into());
        }
        runtime = runtime.with_temp_file_path(path.to_path_buf());
    }
    let session = if args.memory_limit_bytes.is_some() || args.spill_dir.is_some() {
        RustSparkSession::with_config_and_runtime(config, runtime.build_arc()?)
    } else {
        RustSparkSession::with_config(config)
    };
    Ok(Arc::new(session))
}
