use std::{
    fs::{self, File},
    io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};

use super::ReplicaStore;

// Race fallback: a second writer may publish identical immutable bytes while
// we are repairing. Compare bounded buffers instead of collecting the object.
fn equal_files(source: &Path, target: &Path) -> io::Result<bool> {
    let mut from = BufReader::new(File::open(source)?);
    let mut to = BufReader::new(File::open(target)?);
    if from.get_ref().metadata()?.len() != to.get_ref().metadata()?.len() {
        return Ok(false);
    }
    let mut buffer = [0_u8; 64 * 1024];
    let mut other = [0_u8; 64 * 1024];
    loop {
        let bytes = from.read(&mut buffer)?;
        if bytes == 0 {
            return Ok(true);
        }
        to.read_exact(&mut other[..bytes])?;
        if buffer[..bytes] != other[..bytes] {
            return Ok(false);
        }
    }
}

impl ReplicaStore {
    /// Restore missing copies without overwriting any divergent replica.
    /// Call verify first if the source copy may be corrupt.
    pub fn repair_missing(&self, key: &str) -> io::Result<usize> {
        let nodes = self.placements(key)?;
        let statuses = self.inspect(key)?;
        let missing = statuses.iter().filter(|entry| entry.bytes.is_none()).count();
        if missing == 0 {
            return Ok(0);
        }
        // Missing replicas are safe to rebuild only when the surviving copies
        // agree. Never select the first surviving replica as an authority when
        // other surviving replicas contain different bytes.
        if self.verify(key)?.divergent > 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "Cannot repair missing replicas while surviving copies disagree",
            ));
        }
        let source_node = nodes
            .iter()
            .zip(&statuses)
            .find(|(_, status)| status.bytes.is_some())
            .map(|(node, _)| *node)
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "No surviving replica"))?;
        let source_path = source_node.root.join("objects").join(key);
        if !fs::symlink_metadata(&source_path)?.file_type().is_file() {
            return Err(io::Error::new(ErrorKind::InvalidData, "Unsafe source replica"));
        }
        let mut source = File::open(&source_path)?;
        for (node, status) in nodes.iter().zip(statuses) {
            if status.bytes.is_some() {
                continue;
            }
            let directory = node.root.join("objects");
            let target = directory.join(key);
            source.seek(SeekFrom::Start(0))?;
            let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
            io::copy(&mut source, &mut staged)?;
            staged.as_file().sync_all()?;
            match staged.persist_noclobber(&target) {
                Ok(_) => {}
                Err(error) if error.error.kind() == ErrorKind::AlreadyExists => {
                    if !fs::symlink_metadata(&target)?.file_type().is_file()
                        || !equal_files(&source_path, &target)?
                    {
                        return Err(io::Error::new(
                            ErrorKind::AlreadyExists,
                            "Replica already contains different data or an unsafe entry",
                        ));
                    }
                }
                Err(error) => return Err(error.error),
            }
        }
        Ok(missing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn repairs_missing_copies_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        store.put("part.parquet", b"contents").unwrap();
        let lost = store.placements("part.parquet").unwrap()[0];
        fs::remove_file(lost.root.join("objects/part.parquet")).unwrap();
        assert_eq!(store.repair_missing("part.parquet").unwrap(), 1);
        assert_eq!(store.repair_missing("part.parquet").unwrap(), 0);
        assert_eq!(store.verify("part.parquet").unwrap().healthy, 2);
    }

    #[test]
    fn refuses_to_propagate_ambiguous_corrupt_replicas() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b", "c"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 3).unwrap();
        store.put("events.parquet", b"healthy").unwrap();
        let placement = store.placements("events.parquet").unwrap();
        let missing = placement[0].root.join("objects/events.parquet");
        fs::remove_file(&missing).unwrap();
        fs::write(placement[1].root.join("objects/events.parquet"), b"corrupt").unwrap();

        let error = store.repair_missing("events.parquet").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(!missing.exists());
    }

    #[test]
    fn repairs_multi_megabyte_replicas_without_collecting_source_bytes() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let bytes = vec![0xB5_u8; 3 * 1024 * 1024 + 17];
        store.put("large.parquet", &bytes).unwrap();
        let missing = store.placements("large.parquet").unwrap()[0];
        fs::remove_file(missing.root.join("objects/large.parquet")).unwrap();
        assert_eq!(store.repair_missing("large.parquet").unwrap(), 1);
        assert_eq!(store.get("large.parquet").unwrap(), bytes);
        assert_eq!(store.verify("large.parquet").unwrap().healthy, 2);
        assert_eq!(store.repair_missing("large.parquet").unwrap(), 0);
    }
}
