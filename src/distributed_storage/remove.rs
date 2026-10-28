use std::{
    fs,
    io::{self, ErrorKind},
};

use super::ReplicaStore;

impl ReplicaStore {
    /// Delete the current assigned copies. There are no tombstones, so this
    /// must not race a writer or a migration from an older cluster topology.
    pub fn delete(&self, key: &str) -> io::Result<usize> {
        // Fail before deleting any copy if one of the placements is unsafe.
        let mut existing = Vec::new();
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_file() => existing.push(path),
                Ok(_) => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Replica is not a regular file",
                    ));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        for path in &existing {
            fs::remove_file(path)?;
        }
        Ok(existing.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn delete_is_repeatable_and_removes_data() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        store.put("part.parquet", b"data").unwrap();
        assert_eq!(store.delete("part.parquet").unwrap(), 1);
        assert_eq!(store.delete("part.parquet").unwrap(), 0);
        assert!(store.get("part.parquet").is_err());
    }

    #[test]
    fn unsafe_later_replica_does_not_delete_healthy_first_copy() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        store.put("part.parquet", b"keep").unwrap();
        let placements = store.placements("part.parquet").unwrap();
        let first = placements[0].root.join("objects/part.parquet");
        let second = placements[1].root.join("objects/part.parquet");
        fs::remove_file(&second).unwrap();
        fs::create_dir(&second).unwrap();
        assert_eq!(
            store.delete("part.parquet").unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(fs::read(first).unwrap(), b"keep");
    }
}
