use std::{
    fs,
    io::{self, ErrorKind},
    path::Path,
};

use super::ReplicaStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatus {
    pub node_id: String,
    pub bytes: Option<u64>,
}

fn replica_length(path: &Path) -> io::Result<Option<u64>> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => Ok(Some(meta.len())),
        Ok(_) => Err(io::Error::new(
            ErrorKind::InvalidData,
            "Replica is not a regular file",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

impl ReplicaStore {
    pub fn inspect(&self, key: &str) -> io::Result<Vec<ReplicaStatus>> {
        self.placements(key)?
            .into_iter()
            .map(|node| {
                let path = node.root.join("objects").join(key);
                let bytes = replica_length(&path)?;
                Ok(ReplicaStatus {
                    node_id: node.id.clone(),
                    bytes,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn replica_length_distinguishes_missing_and_unsafe_entries() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("object");
        assert_eq!(replica_length(&path).unwrap(), None);
        fs::write(&path, b"abc").unwrap();
        assert_eq!(replica_length(&path).unwrap(), Some(3));
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert_eq!(
            replica_length(&path).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn reports_missing_and_present_replicas_without_reading_payloads() {
        let root = tempfile::tempdir().unwrap();
        let node = StorageNode::new("a", root.path()).unwrap();
        let store = ReplicaStore::new(vec![node], 1).unwrap();
        assert_eq!(store.inspect("part.parquet").unwrap()[0].bytes, None);
        store.put("part.parquet", b"1234").unwrap();
        assert_eq!(store.inspect("part.parquet").unwrap()[0].bytes, Some(4));
    }
}
