//! Run Ballista's scheduler and executor in one process for development.
//! This uses the real Ballista distributed planner but does not span hosts.
use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use rustspark::RustSparkSession;

fn parse_parallelism(value: &str) -> Result<usize, String> {
    let count = value.parse::<usize>().map_err(|_| {
        "RUSTSPARK_STANDALONE_PARALLELISM must be a positive integer".to_owned()
    })?;
    if count == 0 {
        return Err("RUSTSPARK_STANDALONE_PARALLELISM must be greater than zero".into());
    }
    Ok(count)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parallelism = match std::env::var("RUSTSPARK_STANDALONE_PARALLELISM") {
        Ok(value) => parse_parallelism(&value)?,
        Err(std::env::VarError::NotPresent) => 2,
        Err(error) => return Err(error.into()),
    };
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(parallelism)
        .with_ballista_standalone_parallelism(parallelism);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    // Unlike remote(url), standalone starts Ballista in this same process.
    let context = SessionContext::standalone_with_state(state).await?;
    let session = RustSparkSession::from_context(context);
    if let Ok(path) = std::env::var("RUSTSPARK_PARQUET") {
        session.register_parquet("data", &path).await?;
    }
    let query = std::env::var("RUSTSPARK_SQL")
        .unwrap_or_else(|_| "SELECT SUM(id) AS total FROM (VALUES (1), (2), (3)) AS t(id)".into());
    session.sql(&query).await?.show().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_standalone_parallelism() {
        assert_eq!(parse_parallelism("1").unwrap(), 1);
        assert_eq!(parse_parallelism("4").unwrap(), 4);
        for bad in ["0", "-1", "invalid", "1.5"] {
            assert!(parse_parallelism(bad).is_err(), "{bad}");
        }
    }
}
