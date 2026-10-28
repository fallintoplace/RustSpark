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
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        store
            .create_snapshot("sales", 1, vec!["p1.parquet".into()])
            .unwrap();
        assert_eq!(
            store.read_snapshot("sales", 1).unwrap().objects(),
            ["p1.parquet"]
        );
        assert!(
            store
                .create_snapshot("sales", 1, vec!["p2.parquet".into()])
                .is_err()
        );
        assert!(store.read_snapshot("sales", 0).is_err());
    }
}

impl ReplicaStore {
    /// Publish a new generation containing the previous manifest's objects
    /// followed by new parts. Existing generations are never modified.
    pub fn create_snapshot_from(
        &self,
        table: &str,
        base_generation: u64,
        new_generation: u64,
        appended: Vec<String>,
    ) -> io::Result<()> {
        if new_generation <= base_generation {
            return Err(invalid("New snapshot generation must exceed the base"));
        }
        let base = self.read_snapshot(table, base_generation)?;
        let mut objects = base.objects().to_vec();
        objects.extend(appended);
        self.create_snapshot(table, new_generation, objects)
    }
}

#[cfg(test)]
mod incremental_snapshot_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn creates_new_generation_without_modifying_previous_manifest() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.create_snapshot("sales", 1, vec!["one.parquet".into()]).unwrap();
        store.create_snapshot_from("sales", 1, 2, vec!["two.parquet".into()]).unwrap();
        assert_eq!(store.read_snapshot("sales", 1).unwrap().objects(), ["one.parquet"]);
        assert_eq!(
            store.read_snapshot("sales", 2).unwrap().objects(),
            ["one.parquet", "two.parquet"]
        );
        assert!(store.create_snapshot_from("sales", 1, 3, vec!["one.parquet".into()]).is_err());
        assert!(store.create_snapshot_from("sales", 1, 1, vec![]).is_err());
    }
}
