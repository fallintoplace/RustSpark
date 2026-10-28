use std::{
    fs::{self, File},
    io::{self, ErrorKind},
    path::Path,
};

use super::ReplicaStore;

// A source that shrinks or grows during export must not be published as
// a valid immutable object. This checks size stability, not content hashes.
fn require_complete_copy(before: u64, copied: u64, after: u64) -> io::Result<()> {
    if before != copied || before != after {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "Source replica changed size during export",
        ));
    }
    Ok(())
}

fn export_replica(source_path: &Path, destination: &Path, parent: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(source_path)?.file_type().is_file() {
        return Err(io::Error::new(ErrorKind::InvalidData, "Unsafe replica"));
    }
    let mut source = File::open(source_path)?;
    let original_size = source.metadata()?.len();
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    // Stream bounded chunks instead of collecting a multi-GB object.
    let bytes = io::copy(&mut source, &mut staged)?;
    require_complete_copy(original_size, bytes, source.metadata()?.len())?;
    staged.as_file().sync_all()?;
    staged
        .persist_noclobber(destination)
        .map_err(|error| error.error)?;
    Ok(())
}

impl ReplicaStore {
    /// Export a replica to an existing local directory without overwrites.
    pub fn export(&self, key: &str, destination: &Path) -> io::Result<()> {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let mut last_error = None;
        for node in self.placements(key)? {
            let source_path = node.root.join("objects").join(key);
            let attempt = export_replica(&source_path, destination, parent);
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
    fn does_not_accept_short_or_growing_object_copies() {
        assert!(require_complete_copy(100, 100, 100).is_ok());
        assert_eq!(
            require_complete_copy(100, 99, 100).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            require_complete_copy(100, 100, 101).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            require_complete_copy(100, 101, 101).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn export_is_atomic_and_does_not_clobber_a_file() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
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

impl ReplicaStore {
    /// Export distinct object keys into an existing regular directory.
    /// Each file is published atomically without overwrite, but the whole
    /// batch is not transactional if a later object fails.
    pub fn export_many(&self, keys: &[&str], directory: &Path) -> io::Result<Vec<std::path::PathBuf>> {
        if !fs::symlink_metadata(directory)?.file_type().is_dir() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "Export destination must be a directory"));
        }
        let mut seen = std::collections::HashSet::new();
        for key in keys {
            self.placements(key)?;
            if !seen.insert(*key) {
                return Err(io::Error::new(ErrorKind::InvalidInput, "Duplicate object export key"));
            }
        }
        let mut outputs = Vec::with_capacity(keys.len());
        for key in keys {
            let output = directory.join(key);
            self.export(key, &output)?;
            outputs.push(output);
        }
        Ok(outputs)
    }
}

#[cfg(test)]
mod batch_export_feature_tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn exports_multiple_files_without_overwriting_previous_outputs() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("a.parquet", b"a").unwrap();
        store.put("b.parquet", b"b").unwrap();
        let directory = root.path().join("exported");
        fs::create_dir(&directory).unwrap();
        assert!(store.export_many(&["a.parquet", "a.parquet"], &directory).is_err());
        assert!(store.export_many(&["a.parquet", "../bad"], &directory).is_err());
        assert!(fs::read_dir(&directory).unwrap().next().is_none());
        let files = store.export_many(&["a.parquet", "b.parquet"], &directory).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(fs::read(&files[0]).unwrap(), b"a");
        assert_eq!(fs::read(&files[1]).unwrap(), b"b");
        assert_eq!(
            store.export_many(&["a.parquet"], &directory).unwrap_err().kind(),
            ErrorKind::AlreadyExists
        );
    }
}
