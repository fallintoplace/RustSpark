use std::sync::Arc;

use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionConfig};
use rustspark::RustSparkSession;

use super::Args;

fn build_session_config(args: &Args) -> SessionConfig {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = args.parquet_pushdown_filters;
    config.options_mut().execution.parquet.reorder_filters = args.parquet_reorder_filters;
    let config = if let Some(batch_size) = args.batch_size {
        config.with_batch_size(batch_size)
    } else {
        config
    };
    if let Some(partitions) = args.target_partitions {
        config.with_target_partitions(partitions)
    } else {
        config
    }
}

pub(super) fn new_session(
    args: &Args,
) -> Result<Arc<RustSparkSession>, Box<dyn std::error::Error>> {
    let config = build_session_config(args);
    let session = if let Some(limit) = args.memory_limit_bytes {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(limit, 1.0)
            .build_arc()?;
        Arc::new(RustSparkSession::with_config_and_runtime(config, runtime))
    } else {
        Arc::new(RustSparkSession::with_config(config))
    };
    Ok(session)
}
