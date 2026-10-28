use std::{
    fs,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use super::{ReplicaStore, invalid};

impl ReplicaStore {
    /// Materialize immutable snapshot parts in a fresh directory.
    /// All executors must see the same absolute path when used with Ballista.
    pub fn materialize_snapshot(
        &self,
        table: &str,
        generation: u64,
        destination: &Path,
    ) -> io::Result<Vec<PathBuf>> {
        let manifest = self.read_snapshot(table, generation)?;
        if manifest
            .objects()
            .iter()
            .any(|key| !key.ends_with(".parquet"))
        {
            return Err(invalid("Snapshot contains a non-Parquet object"));
        }
        match fs::symlink_metadata(destination) {
            Ok(_) => {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "Destination exists",
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let stage = tempfile::Builder::new()
            .prefix(".rustspark-snapshot-")
            .tempdir_in(parent)?;
        let private = stage.path().join("parts");
        fs::create_dir(&private)?;
        let mut filenames = Vec::with_capacity(manifest.objects().len());
        for (index, key) in manifest.objects().iter().enumerate() {
            let filename = format!("part-{index:06}.parquet");
            self.export(key, &private.join(&filename))?;
            filenames.push(filename);
        }
        // This directory rename is atomic on the destination filesystem.
        match fs::symlink_metadata(destination) {
            Ok(_) => {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "Destination appeared",
                ));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(private, destination)?;
        Ok(filenames
            .into_iter()
            .map(|filename| destination.join(filename))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn publishes_complete_snapshot_parts_to_a_new_directory() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        store.put("first.parquet", b"first").unwrap();
        store.put("second.parquet", b"second").unwrap();
        store
            .create_snapshot(
                "sales",
                1,
                vec!["first.parquet".into(), "second.parquet".into()],
            )
            .unwrap();
        let destination = root.path().join("query");
        let paths = store
            .materialize_snapshot("sales", 1, &destination)
            .unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(fs::read(&paths[0]).unwrap(), b"first");
        assert_eq!(fs::read(&paths[1]).unwrap(), b"second");
        assert!(
            store
                .materialize_snapshot("sales", 1, &destination)
                .is_err()
        );
    }

    #[test]
    fn rejects_non_parquet_manifest_before_publishing_any_parts() {
        let root = tempfile::tempdir().unwrap();
        let store =
            ReplicaStore::new(vec![StorageNode::new("a", root.path()).unwrap()], 1).unwrap();
        store.put("good.parquet", b"big source").unwrap();
        store
            .create_snapshot(
                "mixed",
                1,
                vec!["good.parquet".into(), "not-parquet.csv".into()],
            )
            .unwrap();
        let destination = root.path().join("should-not-exist");
        assert!(
            store
                .materialize_snapshot("mixed", 1, &destination)
                .is_err()
        );
        assert!(!destination.exists());
    }
}
