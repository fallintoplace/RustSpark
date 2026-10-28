use std::{
    fs,
    io::{self, ErrorKind},
    path::Path,
};

use super::ReplicaStore;

fn read_regular_replica(path: &Path) -> io::Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Replica is not a regular file",
        ));
    }
    fs::read(path)
}

impl ReplicaStore {
    /// Open the first available replica as a File so callers can read large
    /// objects with bounded memory. Failover happens at open time only.
    /// An I/O error during later streaming is returned to the caller.
    pub fn open_object(&self, key: &str) -> io::Result<fs::File> {
        let mut last_error = None;
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            let attempt = (|| {
                if !fs::symlink_metadata(&path)?.file_type().is_file() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Replica is not a regular file",
                    ));
                }
                fs::File::open(path)
            })();
            match attempt {
                Ok(file) => return Ok(file),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| io::Error::new(ErrorKind::NotFound, "Object not found")))
    }

    /// Try replicas in stable order. Unavailable roots and missing objects
    /// fall through to the next replica without touching the catalog.
    pub fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        let mut last_error = None;
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            let attempt = read_regular_replica(&path);
            match attempt {
                Ok(bytes) => return Ok(bytes),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| io::Error::new(ErrorKind::NotFound, "Object not found")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn opens_a_surviving_replica_for_streaming_reads() {
        use std::io::Read;

        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let content = vec![0x9a_u8; 2 * 1024 * 1024 + 1];
        store.put("events.parquet", &content).unwrap();
        let first = store.placements("events.parquet").unwrap()[0];
        fs::remove_file(first.root.join("objects/events.parquet")).unwrap();
        let mut result = Vec::new();
        store
            .open_object("events.parquet")
            .unwrap()
            .read_to_end(&mut result)
            .unwrap();
        assert_eq!(result, content);
        assert_eq!(
            store.open_object("absent.parquet").unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn reads_surviving_replica_when_primary_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        store.put("events.parquet", b"data").unwrap();
        let primary = store.placements("events.parquet").unwrap()[0];
        fs::remove_file(primary.root.join("objects/events.parquet")).unwrap();
        assert_eq!(store.get("events.parquet").unwrap(), b"data");
        assert!(store.get("missing.parquet").is_err());
    }
}
