use std::env;

use ballista::prelude::*;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{SessionConfig, SessionContext},
};

fn parse_partitions(input: &str) -> Result<usize, String> {
    let partitions = input
        .parse::<usize>()
        .map_err(|_| "RUSTSPARK_TARGET_PARTITIONS must be a positive integer".to_owned())?;
    if !(1..=65_536).contains(&partitions) {
        return Err("RUSTSPARK_TARGET_PARTITIONS must be between 1 and 65536".into());
    }
    Ok(partitions)
}

pub async fn connect(url: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let partitions = match env::var("RUSTSPARK_TARGET_PARTITIONS") {
        Ok(value) => Some(parse_partitions(&value)?),
        Err(env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let mut config = SessionConfig::new_with_ballista();
    if let Some(partitions) = partitions {
        config = config.with_target_partitions(partitions);
    }

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    Ok(SessionContext::remote_with_state(url, state).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_positive_partition_counts() {
        assert_eq!(parse_partitions("1").unwrap(), 1);
        assert_eq!(parse_partitions("16").unwrap(), 16);
        assert_eq!(parse_partitions("65536").unwrap(), 65_536);
    }

    #[test]
    fn rejects_zero_and_invalid_partition_counts() {
        for value in ["0", "", "bad", "-3", "1.5", "65537"] {
            assert!(parse_partitions(value).is_err(), "{value}");
        }
    }
}
