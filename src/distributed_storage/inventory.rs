use std::{
    fs,
    io::{self, ErrorKind},
};

use super::ReplicaStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaStatus {
    pub node_id: String,
    pub bytes: Option<u64>,
}

impl ReplicaStore {
    pub fn inspect(&self, key: &str) -> io::Result<Vec<ReplicaStatus>> {
        self.placements(key)?
            .into_iter()
            .map(|node| {
                let path = node.root.join("objects").join(key);
                let bytes = match fs::symlink_metadata(path) {
                    Ok(meta) if meta.file_type().is_file() => Some(meta.len()),
                    Ok(_) => {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "Replica is not a regular file",
                        ));
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => None,
                    Err(error) => return Err(error),
                };
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
    fn reports_missing_and_present_replicas_without_reading_payloads() {
        let root = tempfile::tempdir().unwrap();
        let node = StorageNode::new("a", root.path()).unwrap();
        let store = ReplicaStore::new(vec![node], 1).unwrap();
        assert_eq!(store.inspect("part.parquet").unwrap()[0].bytes, None);
        store.put("part.parquet", b"1234").unwrap();
        assert_eq!(store.inspect("part.parquet").unwrap()[0].bytes, Some(4));
    }
}
