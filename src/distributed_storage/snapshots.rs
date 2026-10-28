use std::io;

use super::{ReplicaStore, invalid, manifest::TableManifest, validate_key};

pub(super) fn snapshot_key(table: &str, generation: u64) -> io::Result<String> {
    validate_key(table)?;
    if generation == 0 {
        return Err(invalid("Snapshot generation must be positive"));
    }
    let key = format!("_snapshot_{table}_{generation:020}");
    validate_key(&key)?;
    Ok(key)
}

impl ReplicaStore {
    pub fn create_snapshot(
        &self,
        table: &str,
        generation: u64,
        objects: Vec<String>,
    ) -> io::Result<()> {
        let manifest = TableManifest::new(table, objects)?;
        self.put(&snapshot_key(table, generation)?, &manifest.encode())
    }

    pub fn read_snapshot(&self, table: &str, generation: u64) -> io::Result<TableManifest> {
        let contents = self.get(&snapshot_key(table, generation)?)?;
        let manifest = TableManifest::decode(&contents)?;
        if manifest.table() != table {
            return Err(invalid("Snapshot refers to another table"));
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn publishes_one_immutable_snapshot_generation() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.create_snapshot("sales", 1, vec!["p1.parquet".into()]).unwrap();
        assert_eq!(
            store.read_snapshot("sales", 1).unwrap().objects(),
            ["p1.parquet"]
        );
        assert!(store.create_snapshot("sales", 1, vec!["p2.parquet".into()]).is_err());
        assert!(store.read_snapshot("sales", 0).is_err());
    }
}
