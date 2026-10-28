use std::{
    fs,
    io::{self, ErrorKind},
};

use super::ReplicaStore;

impl ReplicaStore {
    /// Delete the current assigned copies. There are no tombstones, so this
    /// must not race a writer or a migration from an older cluster topology.
    pub fn delete(&self, key: &str) -> io::Result<usize> {
        let mut deleted = 0;
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_file() => {
                    fs::remove_file(path)?;
                    deleted += 1;
                }
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
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn delete_is_repeatable_and_removes_data() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("part.parquet", b"data").unwrap();
        assert_eq!(store.delete("part.parquet").unwrap(), 1);
        assert_eq!(store.delete("part.parquet").unwrap(), 0);
        assert!(store.get("part.parquet").is_err());
    }
}
