//! Build a Ballista-aware remote session with an optional task partition count.
use std::{collections::HashSet, env, time::Duration};

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

fn parse_endpoints(primary: &str, configured: Option<&str>) -> Result<Vec<String>, String> {
    let Some(configured) = configured else {
        return Ok(vec![primary.to_owned()]);
    };
    let candidates: Vec<String> = configured.split(',').map(|part| part.trim().to_owned()).collect();
    if candidates.len() > 16 {
        return Err("RUSTSPARK_SCHEDULER_URLS accepts at most 16 endpoints".into());
    }
    let mut seen = HashSet::new();
    for endpoint in &candidates {
        if endpoint.is_empty() || endpoint.chars().any(char::is_whitespace) {
            return Err("RUSTSPARK_SCHEDULER_URLS contains an empty or invalid endpoint".into());
        }
        if !seen.insert(endpoint.to_ascii_lowercase()) {
            return Err(format!("Duplicate Ballista scheduler endpoint: {endpoint}"));
        }
    }
    Ok(candidates)
}

fn parse_connect_timeout(configured: Option<&str>, candidate_count: usize) -> Result<Option<Duration>, String> {
    let Some(configured) = configured else {
        return Ok((candidate_count > 1).then(|| Duration::from_secs(10)));
    };
    let seconds = configured
        .parse::<u64>()
        .map_err(|_| "RUSTSPARK_SCHEDULER_CONNECT_TIMEOUT_SECS must be a positive integer".to_owned())?;
    if !(1..=300).contains(&seconds) {
        return Err("RUSTSPARK_SCHEDULER_CONNECT_TIMEOUT_SECS must be between 1 and 300".into());
    }
    Ok(Some(Duration::from_secs(seconds)))
}

fn optional_env(name: &str) -> Result<Option<String>, env::VarError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

fn scheduler_address(endpoint: &str) -> Result<String, String> {
    let parsed = url::Url::parse(endpoint).map_err(|error| error.to_string())?;
    let host = parsed.host().ok_or("Ballista endpoint must contain a hostname")?;
    let port = parsed.port().unwrap_or(50050);
    Ok(format!("{host}:{port}"))
}

async fn probe_reachable(endpoint: &str, limit: Duration) -> Result<(), String> {
    let address = scheduler_address(endpoint)?;
    match tokio::time::timeout(limit, tokio::net::TcpStream::connect(address)).await {
        Ok(Ok(connection)) => {
            drop(connection);
            Ok(())
        }
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("connection timed out after {} seconds", limit.as_secs())),
    }
}

async fn connect_endpoint(
    endpoint: &str,
    partitions: Option<usize>,
) -> Result<SessionContext, String> {
    let mut config = SessionConfig::new_with_ballista();
    if let Some(partitions) = partitions {
        config = config.with_target_partitions(partitions);
    }
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::remote_with_state(endpoint, state)
        .await
        .map_err(|error| error.to_string())
}

pub async fn connect(url: &str) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let partitions = match env::var("RUSTSPARK_TARGET_PARTITIONS") {
        Ok(value) => Some(parse_partitions(&value)?),
        Err(env::VarError::NotPresent) => None,
        Err(error) => return Err(error.into()),
    };
    let configured = optional_env("RUSTSPARK_SCHEDULER_URLS")?;
    let endpoints = parse_endpoints(url, configured.as_deref())?;
    let configured_timeout = optional_env("RUSTSPARK_SCHEDULER_CONNECT_TIMEOUT_SECS")?;
    let timeout = parse_connect_timeout(configured_timeout.as_deref(), endpoints.len())?;
    let mut failures = Vec::new();

    for (index, endpoint) in endpoints.iter().enumerate() {
        if let Some(limit) = timeout {
            if let Err(message) = probe_reachable(endpoint, limit).await {
                failures.push(format!("{endpoint}: {message}"));
                continue;
            }
        }
        let result = connect_endpoint(endpoint, partitions).await;
        match result {
            Ok(context) => {
                if index > 0 {
                    eprintln!("Connected to fallback Ballista scheduler: {endpoint}");
                }
                return Ok(context);
            }
            Err(message) => failures.push(format!("{endpoint}: {message}")),
        }
    }

    Err(format!("Unable to connect to a Ballista scheduler: {}", failures.join(" | ")).into())
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

    #[test]
    fn preserves_single_scheduler_when_fallbacks_are_unset() {
        assert_eq!(parse_endpoints("df://primary:50050", None).unwrap(), vec!["df://primary:50050"]);
        assert_eq!(parse_connect_timeout(None, 1).unwrap(), None);
    }

    #[test]
    fn parses_ordered_scheduler_fallbacks() {
        let urls = parse_endpoints("df://ignored:50050", Some("df://a:50050, df://b:50050")).unwrap();
        assert_eq!(urls, ["df://a:50050", "df://b:50050"]);
        assert_eq!(parse_connect_timeout(None, urls.len()).unwrap(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn rejects_invalid_scheduler_fallback_lists() {
        for value in ["", "df://a:50050,", ",df://a:50050", "df://a:50050, df://A:50050", "df://host with spaces:50050"] {
            assert!(parse_endpoints("df://ignored:50050", Some(value)).is_err(), "{value}");
        }
        let many = vec!["df://host:50050"; 17].join(",");
        assert!(parse_endpoints("df://ignored:50050", Some(&many)).is_err());
    }

    #[test]
    fn scheduler_probe_uses_ballista_url_defaults() {
        assert_eq!(scheduler_address("df://scheduler").unwrap(), "scheduler:50050");
        assert_eq!(scheduler_address("df://scheduler:50100").unwrap(), "scheduler:50100");
        assert!(scheduler_address("df:///no-host").is_err());
    }

    #[tokio::test]
    async fn scheduler_probe_detects_live_and_closed_listeners() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("df://{}", listener.local_addr().unwrap());
        assert!(probe_reachable(&endpoint, Duration::from_secs(1)).await.is_ok());
        drop(listener);
        assert!(probe_reachable(&endpoint, Duration::from_secs(1)).await.is_err());
    }

    #[test]
    fn validates_connection_timeout() {
        assert_eq!(parse_connect_timeout(Some("1"), 2).unwrap(), Some(Duration::from_secs(1)));
        assert_eq!(parse_connect_timeout(Some("300"), 1).unwrap(), Some(Duration::from_secs(300)));
        for invalid in ["0", "301", "", "none", "-1", "1.5"] {
            assert!(parse_connect_timeout(Some(invalid), 2).is_err(), "{invalid}");
        }
    }
}
