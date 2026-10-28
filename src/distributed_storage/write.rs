use std::{
    fs::{self, File},
    io::{self, Read, Seek, Write},
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

// Compare an existing object with a staged stream using bounded buffers.
fn existing_matches_staged(path: &Path, source: &mut File, length: u64) -> io::Result<Option<bool>> {
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
    if metadata.len() != length {
        return Ok(Some(false));
    }
    source.rewind()?;
    let mut existing = File::open(path)?;
    let mut from_source = [0_u8; 64 * 1024];
    let mut from_existing = [0_u8; 64 * 1024];
    loop {
        let count = source.read(&mut from_source)?;
        if count == 0 {
            return Ok(Some(true));
        }
        existing.read_exact(&mut from_existing[..count])?;
        if from_existing[..count] != from_source[..count] {
            return Ok(Some(false));
        }
    }
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
    /// Copy a large source once to a temporary file and publish it to the
    /// assigned replicas without collecting the object in memory.
    ///
    /// The temporary spool needs enough free disk space for the input size.
    /// As with put(), partial replication is possible if a later node fails.
    pub fn put_reader(&self, key: &str, mut input: impl Read) -> io::Result<u64> {
        let nodes = self.placements(key)?;
        let mut source = tempfile::tempfile()?;
        let length = io::copy(&mut input, &mut source)?;
        for node in nodes {
            let directory = node.root.join("objects");
            let target = directory.join(key);
            match existing_matches_staged(&target, &mut source, length)? {
                Some(true) => continue,
                Some(false) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Replica already contains different data",
                    ));
                }
                None => {}
            }
            source.rewind()?;
            let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
            io::copy(&mut source, &mut staged)?;
            staged.as_file().sync_all()?;
            match staged.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    if existing_matches_staged(&target, &mut source, length)? != Some(true) {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "Replica already contains different data or an unsafe entry",
                        ));
                    }
                }
                Err(error) => return Err(error.error),
            }
        }
        Ok(length)
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
    fn streamed_write_is_replicated_and_idempotent() {
        use std::io::Cursor;

        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let content = vec![0x5a_u8; 3 * 1024 * 1024 + 37];
        let bytes = store.put_reader("large.parquet", Cursor::new(&content)).unwrap();
        assert_eq!(bytes, content.len() as u64);
        assert_eq!(store.put_reader("large.parquet", Cursor::new(&content)).unwrap(), bytes);
        assert_eq!(store.verify("large.parquet").unwrap().healthy, 2);
        let mut changed = content.clone();
        changed[1024 * 1024] ^= 1;
        assert_eq!(
            store.put_reader("large.parquet", Cursor::new(changed)).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(store.get("large.parquet").unwrap(), content);
    }

    #[test]
    fn reader_error_does_not_publish_a_partial_replica() {
        struct FailedReader;

        impl Read for FailedReader {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "input failure"))
            }
        }

        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        ).unwrap();
        assert!(store.put_reader("failed.parquet", FailedReader).is_err());
        assert!(store.get("failed.parquet").is_err());
    }

    #[test]
    fn idempotent_large_write_detects_same_length_divergence() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
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
