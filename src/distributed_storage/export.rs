use std::{
    fs::{self, File},
    io::{self, ErrorKind},
    path::Path,
};

use super::ReplicaStore;

impl ReplicaStore {
    /// Export a replica to an existing local directory without overwrites.
    pub fn export(&self, key: &str, destination: &Path) -> io::Result<()> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let mut last_error = None;
        for node in self.placements(key)? {
            let source_path = node.root.join("objects").join(key);
            let attempt = (|| -> io::Result<()> {
                if !fs::symlink_metadata(&source_path)?.file_type().is_file() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "Unsafe replica"));
                }
                let mut source = File::open(&source_path)?;
                let mut staged = tempfile::NamedTempFile::new_in(parent)?;
                // Stream bounded chunks instead of collecting a multi-GB object.
                io::copy(&mut source, &mut staged)?;
                staged.as_file().sync_all()?;
                staged.persist_noclobber(destination).map_err(|error| error.error)?;
                Ok(())
            })();
            match attempt {
                Ok(()) => return Ok(()),
                // Never try another replica after an output collision.
                Err(error) if error.kind() == ErrorKind::AlreadyExists => return Err(error),
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
    use std::fs;

    #[test]
    fn export_is_atomic_and_does_not_clobber_a_file() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("part.parquet", b"object").unwrap();
        let destination = root.path().join("export.parquet");
        store.export("part.parquet", &destination).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"object");
        assert!(store.export("part.parquet", &destination).is_err());
        assert_eq!(fs::read(&destination).unwrap(), b"object");
    }

    #[test]
    fn streams_from_surviving_replica_when_primary_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let payload = vec![42_u8; 2 * 1024 * 1024];
        store.put("large.parquet", &payload).unwrap();
        let primary = store.placements("large.parquet").unwrap()[0];
        fs::remove_file(primary.root.join("objects/large.parquet")).unwrap();
        let output = root.path().join("export.parquet");
        store.export("large.parquet", &output).unwrap();
        assert_eq!(fs::metadata(&output).unwrap().len(), payload.len() as u64);
        assert_eq!(fs::read(output).unwrap(), payload);
    }
}
