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

impl ReplicaStore {
    /// Return the object length without reading its contents. Missing copies
    /// are tolerated; conflicting surviving lengths are an error. Equal lengths
    /// do not prove that the contents are equal; use verify() for that.
    pub fn object_size(&self, key: &str) -> io::Result<u64> {
        let mut length = None;
        for replica in self.inspect(key)? {
            if let Some(bytes) = replica.bytes {
                match length {
                    Some(previous) if previous != bytes => {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "Surviving replicas disagree on object length",
                        ));
                    }
                    None => length = Some(bytes),
                    _ => {}
                }
            }
        }
        length.ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Object not found"))
    }
}

#[cfg(test)]
mod object_size_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn reports_length_and_rejects_divergent_metadata() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"]
            .into_iter()
            .map(|id| {
                let directory = root.path().join(id);
                fs::create_dir(&directory).unwrap();
                StorageNode::new(id, directory).unwrap()
            })
            .collect();
        let store = ReplicaStore::new(nodes, 2).unwrap();
        store.put("p.parquet", b"12345").unwrap();
        assert_eq!(store.object_size("p.parquet").unwrap(), 5);
        let first = store.placements("p.parquet").unwrap()[0];
        fs::remove_file(first.root.join("objects/p.parquet")).unwrap();
        assert_eq!(store.object_size("p.parquet").unwrap(), 5);
        fs::write(first.root.join("objects/p.parquet"), b"no").unwrap();
        assert_eq!(store.object_size("p.parquet").unwrap_err().kind(), ErrorKind::InvalidData);
        assert_eq!(store.object_size("missing").unwrap_err().kind(), ErrorKind::NotFound);
    }
}
