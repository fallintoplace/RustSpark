use std::{
    fs,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{ReplicaStore, invalid};

impl ReplicaStore {
    /// Keep the default sequential behavior for existing callers.
    pub fn materialize_snapshot(
        &self,
        table: &str,
        generation: u64,
        destination: &Path,
    ) -> io::Result<Vec<PathBuf>> {
        self.materialize_snapshot_with_parallelism(table, generation, destination, 1)
    }

    /// Copy immutable snapshot parts concurrently with bounded I/O fan-out.
    /// A fresh destination is published only after every worker succeeds.
    pub fn materialize_snapshot_with_parallelism(
        &self,
        table: &str,
        generation: u64,
        destination: &Path,
        parallelism: usize,
    ) -> io::Result<Vec<PathBuf>> {
        if !(1..=32).contains(&parallelism) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Snapshot copy parallelism must be between 1 and 32",
            ));
        }
        let manifest = self.read_snapshot(table, generation)?;
        if manifest.objects().iter().any(|key| !key.ends_with(".parquet")) {
            return Err(invalid("Snapshot contains a non-Parquet object"));
        }
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
        let filenames = (0..manifest.objects().len())
            .map(|index| format!("part-{index:06}.parquet"))
            .collect::<Vec<_>>();

        if parallelism == 1 {
            for (key, filename) in manifest.objects().iter().zip(&filenames) {
                self.export(key, &private.join(filename))?;
            }
        } else {
            let next = AtomicUsize::new(0);
            let failure = Mutex::new(None::<io::Error>);
            std::thread::scope(|scope| {
                for _ in 0..parallelism.min(filenames.len()) {
                    let keys = manifest.objects();
                    let filenames = &filenames;
                    let private = &private;
                    let next = &next;
                    let failure = &failure;
                    scope.spawn(move || {
                        loop {
                            let index = next.fetch_add(1, Ordering::Relaxed);
                            if index >= keys.len() {
                                break;
                            }
                            if let Err(error) = self.export(
                                &keys[index],
                                &private.join(&filenames[index]),
                            ) {
                                let mut slot = failure.lock().expect("worker error mutex poisoned");
                                if slot.is_none() {
                                    *slot = Some(error);
                                }
                                break;
                            }
                        }
                    });
                }
            });
            if let Some(error) = failure
                .into_inner()
                .map_err(|_| io::Error::other("Snapshot worker error lock poisoned"))?
            {
                return Err(error);
            }
        }

        // Publish only a complete immutable generation.
        match fs::symlink_metadata(destination) {
            Ok(_) => return Err(io::Error::new(ErrorKind::AlreadyExists, "Destination appeared")),
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

    #[test]
    fn copies_parts_concurrently_in_manifest_order() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        let keys = (0..8)
            .map(|i| format!("object-{i}.parquet"))
            .collect::<Vec<_>>();
        for (index, key) in keys.iter().enumerate() {
            store.put(key, &[index as u8; 1024]).unwrap();
        }
        store.create_snapshot("events", 1, keys).unwrap();
        let destination = root.path().join("parallel-output");
        let paths = store
            .materialize_snapshot_with_parallelism("events", 1, &destination, 4)
            .unwrap();
        assert_eq!(paths.len(), 8);
        for (index, path) in paths.iter().enumerate() {
            assert_eq!(fs::read(path).unwrap(), vec![index as u8; 1024]);
        }
        assert!(store
            .materialize_snapshot_with_parallelism("events", 1, &destination, 4)
            .is_err());
        assert!(store
            .materialize_snapshot_with_parallelism("events", 1, &root.path().join("bad"), 0)
            .is_err());
    }

    #[test]
    fn failure_in_one_worker_does_not_publish_partial_generation() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("good.parquet", b"good").unwrap();
        store.put("missing.parquet", b"missing").unwrap();
        store.create_snapshot(
            "events",
            2,
            vec!["good.parquet".into(), "missing.parquet".into()],
        ).unwrap();
        fs::remove_file(root.path().join("objects/missing.parquet")).unwrap();
        let destination = root.path().join("incomplete");
        assert!(store
            .materialize_snapshot_with_parallelism("events", 2, &destination, 2)
            .is_err());
        assert!(!destination.exists());
    }

    #[test]
    fn rejects_non_parquet_manifest_before_publishing_any_parts() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        ).unwrap();
        store.put("good.parquet", b"big source").unwrap();
        store.create_snapshot(
            "mixed",
            1,
            vec!["good.parquet".into(), "not-parquet.csv".into()],
        ).unwrap();
        let destination = root.path().join("should-not-exist");
        assert!(store.materialize_snapshot("mixed", 1, &destination).is_err());
        assert!(!destination.exists());
    }
}
