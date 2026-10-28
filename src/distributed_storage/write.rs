use std::{
    fs,
    io::{self, Write},
};

use super::ReplicaStore;

impl ReplicaStore {
    /// Publish an immutable object to every selected replica. Partial replicas
    /// are possible if a later node fails; retrying the same bytes repairs them.
    pub fn put(&self, key: &str, bytes: &[u8]) -> io::Result<()> {
        for node in self.placements(key)? {
            let directory = node.root.join("objects");
            let target = directory.join(key);
            let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
            staged.write_all(bytes)?;
            staged.as_file().sync_all()?;
            match staged.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                    let existing = fs::symlink_metadata(&target)?;
                    if !existing.file_type().is_file() || fs::read(&target)? != bytes {
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
}
