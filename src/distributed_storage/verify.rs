use std::{
    fs,
    io::{self, ErrorKind},
};

use super::ReplicaStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaVerification {
    pub healthy: usize,
    pub missing: usize,
    pub divergent: usize,
}

impl ReplicaStore {
    /// Compare copies byte-for-byte; no replica is assumed to be authoritative.
    pub fn verify(&self, key: &str) -> io::Result<ReplicaVerification> {
        let reference = self.get(key)?;
        let mut result = ReplicaVerification {
            healthy: 0,
            missing: 0,
            divergent: 0,
        };
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_file() => {
                    if fs::read(path)? == reference {
                        result.healthy += 1;
                    } else {
                        result.divergent += 1;
                    }
                }
                Ok(_) => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Replica is not a regular file",
                    ));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => result.missing += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn identifies_missing_and_different_copies() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b", "c"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 3).unwrap();
        store.put("part.parquet", b"same").unwrap();
        let assigned = store.placements("part.parquet").unwrap();
        fs::remove_file(assigned[1].root.join("objects/part.parquet")).unwrap();
        fs::write(assigned[2].root.join("objects/part.parquet"), b"other").unwrap();
        let result = store.verify("part.parquet").unwrap();
        assert_eq!(result.healthy, 1);
        assert_eq!(result.missing, 1);
        assert_eq!(result.divergent, 1);
    }
}
