//! Optional replicated Parquet snapshot registration for the Ballista client.
use std::{env, error::Error, path::Path};

use rustspark::{RustSparkSession, distributed_storage::ReplicaStore};

const SETTINGS: [&str; 5] = [
    "RUSTSPARK_STORAGE_TOPOLOGY_FILE",
    "RUSTSPARK_STORAGE_REPLICAS",
    "RUSTSPARK_STORAGE_TABLE",
    "RUSTSPARK_STORAGE_GENERATION",
    "RUSTSPARK_STORAGE_MATERIALIZE_DIR",
];

fn optional_env(name: &str) -> Result<Option<String>, env::VarError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

pub async fn register(session: &RustSparkSession) -> Result<(), Box<dyn Error>> {
    let values = SETTINGS
        .iter()
        .map(|name| optional_env(name))
        .collect::<Result<Vec<_>, _>>()?;
    if values.iter().all(Option::is_none) {
        return Ok(());
    }
    if values.iter().any(Option::is_none) {
        return Err("All five RUSTSPARK_STORAGE_* settings are required together".into());
    }
    let values: Vec<String> = values.into_iter().map(Option::unwrap).collect();
    let replicas = values[1]
        .parse::<usize>()
        .map_err(|_| "RUSTSPARK_STORAGE_REPLICAS must be a positive integer")?;
    let generation = values[3]
        .parse::<u64>()
        .map_err(|_| "RUSTSPARK_STORAGE_GENERATION must be a positive integer")?;
    let table = values[2].as_str();
    if session.table_exists(table)? {
        return Err(format!("Snapshot table already registered: {table}").into());
    }
    let store = ReplicaStore::from_topology_file(Path::new(&values[0]), replicas)?;
    session
        .register_snapshot_parquet(
            table,
            &store,
            table,
            generation,
            Path::new(&values[4]),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_settings_have_unique_names() {
        let names = SETTINGS.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(names.len(), SETTINGS.len());
    }
}
