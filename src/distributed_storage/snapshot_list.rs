use std::io;

use super::{ReplicaStore, validate_key};

impl ReplicaStore {
    pub fn list_snapshots(&self, table: &str) -> io::Result<Vec<u64>> {
        validate_key(table)?;
        let prefix = format!("_snapshot_{table}_");
        let mut generations = self
            .list_keys()?
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix(&prefix)
                    .and_then(|suffix| suffix.parse::<u64>().ok())
                    .filter(|generation| *generation > 0)
            })
            .collect::<Vec<_>>();
        generations.sort_unstable();
        generations.dedup();
        Ok(generations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn discovers_snapshots_without_mixing_tables() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.create_snapshot("sales", 7, vec!["a.parquet".into()]).unwrap();
        store.create_snapshot("sales", 2, vec!["a.parquet".into()]).unwrap();
        store.create_snapshot("other", 9, vec!["a.parquet".into()]).unwrap();
        assert_eq!(store.list_snapshots("sales").unwrap(), [2, 7]);
        assert!(store.list_snapshots("empty").unwrap().is_empty());
    }
}
