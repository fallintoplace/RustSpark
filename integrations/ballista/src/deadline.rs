use std::{env, time::Duration};

fn parse_deadline(value: &str) -> Result<Duration, String> {
    let seconds = value.parse::<u64>().map_err(|_| {
        "RUSTSPARK_QUERY_TIMEOUT_SECS must be an integer number of seconds".to_owned()
    })?;
    if !(1..=86_400).contains(&seconds) {
        return Err("RUSTSPARK_QUERY_TIMEOUT_SECS must be between 1 and 86400".into());
    }
    Ok(Duration::from_secs(seconds))
}

pub fn configured_deadline() -> Result<Option<Duration>, Box<dyn std::error::Error>> {
    match env::var("RUSTSPARK_QUERY_TIMEOUT_SECS") {
        Ok(value) => Ok(Some(parse_deadline(&value)?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_remote_deadlines() {
        assert_eq!(parse_deadline("1").unwrap(), Duration::from_secs(1));
        assert_eq!(parse_deadline("86400").unwrap(), Duration::from_secs(86400));
    }

    #[test]
    fn rejects_invalid_remote_deadlines() {
        for bad in ["0", "", "-1", "2.5", "86401", "n/a"] {
            assert!(parse_deadline(bad).is_err(), "{bad}");
        }
    }
}
