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
        match fs::symlink_metadata(destination) {
            Ok(_) => return Err(io::Error::new(ErrorKind::AlreadyExists, "Destination exists")),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let stage = tempfile::Builder::new()
            .prefix(".rustspark-snapshot-")
            .tempdir_in(parent)?;
        let private = stage.path().join("parts");
        fs::create_dir(&private)?;
        let mut filenames = Vec::new();
        for (index, key) in manifest.objects().iter().enumerate() {
            if !key.ends_with(".parquet") {
                return Err(invalid("Snapshot contains a non-Parquet object"));
            }
            let filename = format!("part-{index:06}.parquet");
            self.export(key, &private.join(&filename))?;
            filenames.push(filename);
        }
        // This directory rename is atomic on the destination filesystem.
        if fs::symlink_metadata(destination).is_ok() {
            return Err(io::Error::new(ErrorKind::AlreadyExists, "Destination appeared"));
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
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
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
        let paths = store.materialize_snapshot("sales", 1, &destination).unwrap();
        assert_eq!(paths.len(), 2);
        assert_eq!(fs::read(&paths[0]).unwrap(), b"first");
        assert_eq!(fs::read(&paths[1]).unwrap(), b"second");
        assert!(store.materialize_snapshot("sales", 1, &destination).is_err());
    }
}
