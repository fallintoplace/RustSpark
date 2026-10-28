use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

/// Avoid rewriting immutable objects on idempotent retries.
fn existing_matches(path: &Path, expected: &[u8]) -> io::Result<Option<bool>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Replica already contains an unsafe entry",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.len() != expected.len() as u64 {
        return Ok(Some(false));
    }
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 64 * 1024];
    for chunk in expected.chunks(buffer.len()) {
        file.read_exact(&mut buffer[..chunk.len()])?;
        if buffer[..chunk.len()] != *chunk {
            return Ok(Some(false));
        }
    }
    Ok(Some(true))
}

use super::ReplicaStore;

impl ReplicaStore {
    /// Publish an immutable object to every selected replica. Partial replicas
    /// are possible if a later node fails; retrying the same bytes repairs them.
    pub fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        for node in self.placements(key)? {
            let directory = node.root.join("objects");
            let target = directory.join(key);
            match existing_matches(&target, bytes)? {
                Some(true) => continue,
                Some(false) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Replica already contains different data",
                    ));
                }
                None => {}
            }
            let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
            staged.write_all(bytes)?;
            staged.as_file().sync_all()?;
            match staged.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    // Another writer may have published after the initial check.
                    if existing_matches(&target, bytes)? != Some(true) {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "Replica already contains different data or an unsafe entry",
                        ));
                    }
                }
                Err(error) => return Err(error.error),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn writes_immutable_replicas_and_allows_same_content_retry() {
        let root = tempfile::tempdir().unwrap();
        let node = StorageNode::new("a", root.path()).unwrap();
        let store = ReplicaStore::new(vec![node], 1).unwrap();
        store.put("part.parquet", b"abc").unwrap();
        store.put("part.parquet", b"abc").unwrap();
        assert!(store.put("part.parquet", b"changed").is_err());
        assert_eq!(
            std::fs::read(root.path().join("objects/part.parquet")).unwrap(),
            b"abc"
        );
    }

    #[test]
    fn idempotent_large_write_detects_same_length_divergence() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        let payload = vec![7_u8; 2 * 1024 * 1024];
        store.put("large.parquet", &payload).unwrap();
        store.put("large.parquet", &payload).unwrap();
        let mut different = payload.clone();
        different[1024 * 1024] = 8;
        assert_eq!(
            store.put("large.parquet", &different).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            fs::read(root.path().join("objects/large.parquet")).unwrap(),
            payload
        );
    }
}
