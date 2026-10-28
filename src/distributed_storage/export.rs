use std::{
    io::{self, Write},
    path::Path,
};

use super::ReplicaStore;

impl ReplicaStore {
    /// Export a replica to an existing local directory without overwrites.
    pub fn export(&self, key: &str, destination: &Path) -> io::Result<()> {
        let bytes = self.get(key)?;
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let mut staged = tempfile::NamedTempFile::new_in(parent)?;
        staged.write_all(&bytes)?;
        staged.as_file().sync_all()?;
        staged.persist_noclobber(destination).map_err(|error| error.error)?;
        Ok(())
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
}
