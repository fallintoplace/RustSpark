use std::{
    fs::{self, File},
    io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use super::ReplicaStore;

fn matching_files(left: &Path, right: &Path) -> io::Result<bool> {
    let mut left = BufReader::new(File::open(left)?);
    let mut right = BufReader::new(File::open(right)?);
    if left.get_ref().metadata()?.len() != right.get_ref().metadata()?.len() {
        return Ok(false);
    }
    let mut a = [0_u8; 64 * 1024];
    let mut b = [0_u8; 64 * 1024];
    loop {
        let n = left.read(&mut a)?;
        if n == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..n])?;
        if a[..n] != b[..n] {
            return Ok(false);
        }
    }
}

impl ReplicaStore {
    fn surviving_path(&self, key: &str) -> io::Result<PathBuf> {
        let mut last_error = None;
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_file() => return Ok(path),
                Ok(_) => {
                    last_error = Some(io::Error::new(
                        ErrorKind::InvalidData,
                        "Unsafe source replica",
                    ));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| io::Error::new(ErrorKind::NotFound, "Object not found")))
    }

    /// Copy objects into the target topology. Keep original copies until
    /// the destination has been verified and active readers have switched.
    pub fn copy_to_topology(&self, destination: &ReplicaStore) -> io::Result<usize> {
        let mut copied = 0;
        for key in self.list_keys()? {
            let source_path = self.surviving_path(&key)?;
            let mut source = File::open(&source_path)?;
            for node in destination.placements(&key)? {
                let directory = node.root.join("objects");
                let target = directory.join(&key);
                match fs::symlink_metadata(&target) {
                    Ok(metadata) if metadata.file_type().is_file() => {
                        if matching_files(&source_path, &target)? {
                            continue;
                        }
                        return Err(io::Error::new(
                            ErrorKind::AlreadyExists,
                            "Divergent destination replica",
                        ));
                    }
                    Ok(_) => {
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            "Unsafe destination replica",
                        ));
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
                // Transfer one immutable object at a time without collecting
                // its entire content in memory.
                source.seek(SeekFrom::Start(0))?;
                let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
                io::copy(&mut source, &mut staged)?;
                staged.as_file().sync_all()?;
                match staged.persist_noclobber(&target) {
                    Ok(_) => {}
                    Err(error) if error.error.kind() == ErrorKind::AlreadyExists => {
                        if !fs::symlink_metadata(&target)?.file_type().is_file()
                            || !matching_files(&source_path, &target)?
                        {
                            return Err(io::Error::new(
                                ErrorKind::AlreadyExists,
                                "Divergent destination replica",
                            ));
                        }
                    }
                    Err(error) => return Err(error.error),
                }
            }
            copied += 1;
        }
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;
    use std::fs;

    #[test]
    fn migrates_objects_without_removing_old_copies() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let original = ReplicaStore::new(vec![nodes[0].clone()], 1).unwrap();
        original.put("part.parquet", b"payload").unwrap();
        let expanded = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        assert_eq!(original.copy_to_topology(&expanded).unwrap(), 1);
        assert_eq!(expanded.verify("part.parquet").unwrap().healthy, 2);
        assert_eq!(original.get("part.parquet").unwrap(), b"payload");
    }

    #[test]
    fn large_rebalance_retries_idempotently_without_overwriting_divergent_data() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b", "c"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let original = ReplicaStore::new(nodes[..2].to_vec(), 2).unwrap();
        let bytes = vec![9_u8; 3 * 1024 * 1024 + 13];
        original.put("large.parquet", &bytes).unwrap();
        let expanded = ReplicaStore::new(nodes.to_vec(), 3).unwrap();
        assert_eq!(original.copy_to_topology(&expanded).unwrap(), 1);
        assert_eq!(original.copy_to_topology(&expanded).unwrap(), 1);
        assert_eq!(expanded.verify("large.parquet").unwrap().healthy, 3);
        let conflicting = expanded.placements("large.parquet").unwrap()[2];
        fs::write(conflicting.root.join("objects/large.parquet"), b"different").unwrap();
        assert_eq!(
            original.copy_to_topology(&expanded).unwrap_err().kind(),
            ErrorKind::AlreadyExists
        );
        assert_eq!(original.get("large.parquet").unwrap(), bytes);
    }
}
