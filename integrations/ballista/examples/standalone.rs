use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};
use rustspark::RustSparkSession;

#[path = "../src/remote_inputs.rs"]
mod remote_inputs;

const MAX_STANDALONE_PARALLELISM: usize = 256;

fn default_if_unset(
    value: Result<String, std::env::VarError>,
    fallback: &str,
) -> Result<String, std::env::VarError> {
    match value {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(fallback.to_owned()),
        Err(error) => Err(error),
    }
}

fn parse_parallelism(value: &str) -> Result<usize, String> {
    let count = value.parse::<usize>().map_err(|_| {
        "RUSTSPARK_STANDALONE_PARALLELISM must be a positive integer".to_owned()
    })?;
    if !(1..=MAX_STANDALONE_PARALLELISM).contains(&count) {
        return Err("RUSTSPARK_STANDALONE_PARALLELISM must be between 1 and 256".into());
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

    let context = SessionContext::standalone_with_state(state).await?;
    let session = RustSparkSession::from_context(context);
    match std::env::var("RUSTSPARK_PARQUET") {
        Ok(path) => session.register_parquet("data", &path).await?,
        Err(std::env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    remote_inputs::register(&session).await?;
    let query = default_if_unset(
        std::env::var("RUSTSPARK_SQL"),
        "SELECT SUM(id) AS total FROM (VALUES (1), (2), (3)) AS t(id)",
    )?;
    session.sql(&query).await?.show().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn validates_standalone_parallelism() {
        assert_eq!(parse_parallelism("1").unwrap(), 1);
        assert_eq!(parse_parallelism("4").unwrap(), 4);
        assert_eq!(parse_parallelism("256").unwrap(), 256);
        for bad in ["0", "-1", "invalid", "1.5", "257"] {
            assert!(parse_parallelism(bad).is_err(), "{bad}");
        }
    }
    #[test]
    fn only_unset_sql_uses_the_default() {
        use std::env::VarError;
        assert_eq!(
            default_if_unset(Err(VarError::NotPresent), "fallback").unwrap(),
            "fallback"
        );
        assert!(matches!(
            default_if_unset(Err(VarError::NotUnicode(OsString::from("raw"))), "fallback"),
            Err(VarError::NotUnicode(_))
        ));
    }

}
