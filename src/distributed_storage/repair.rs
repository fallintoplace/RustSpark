use std::io;

use super::ReplicaStore;

impl ReplicaStore {
    /// Restore missing copies without overwriting any divergent replica.
    /// Call verify first if the source copy may be corrupt.
    pub fn repair_missing(&self, key: &str) -> io::Result<usize> {
        let missing = self
            .inspect(key)?
            .iter()
            .filter(|replica| replica.bytes.is_none())
            .count();
        if missing == 0 {
            return Ok(0);
        }
        let bytes = self.get(key)?;
        self.put(key, &bytes)?;
        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn repairs_missing_copies_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        store.put("part.parquet", b"contents").unwrap();
        let lost = store.placements("part.parquet").unwrap()[0];
        fs::remove_file(lost.root.join("objects/part.parquet")).unwrap();
        assert_eq!(store.repair_missing("part.parquet").unwrap(), 1);
        assert_eq!(store.repair_missing("part.parquet").unwrap(), 0);
        assert_eq!(store.verify("part.parquet").unwrap().healthy, 2);
    }
}
