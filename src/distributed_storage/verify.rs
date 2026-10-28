use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, ErrorKind},
    path::Path,
};

use super::ReplicaStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaVerification {
    pub healthy: usize,
    pub missing: usize,
    pub divergent: usize,
}

/// Compare two replica files without allocating buffers proportional to object size.
pub(super) fn same_bytes(left: &Path, right: &Path) -> io::Result<bool> {
    let left_file = File::open(left)?;
    let right_file = File::open(right)?;
    if left_file.metadata()?.len() != right_file.metadata()?.len() {
        return Ok(false);
    }
    let mut left = BufReader::with_capacity(64 * 1024, left_file);
    let mut right = BufReader::with_capacity(64 * 1024, right_file);
    loop {
        let a = left.fill_buf()?;
        let b = right.fill_buf()?;
        if a.is_empty() || b.is_empty() {
            return Ok(a.is_empty() && b.is_empty());
        }
        let length = a.len().min(b.len());
        if a[..length] != b[..length] {
            return Ok(false);
        }
        left.consume(length);
        right.consume(length);
    }
}

impl ReplicaStore {
    /// Compare copies byte-for-byte; no replica is assumed to be authoritative.
    pub fn verify(&self, key: &str) -> io::Result<ReplicaVerification> {
        let placements = self.placements(key)?;
        let mut reference = None;
        // Preserve the existing first-surviving-replica semantics.
        for node in &placements {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_file() => {
                    reference = Some(path);
                    break;
                }
                Ok(_) => return Err(io::Error::new(ErrorKind::InvalidData, "Unsafe replica")),
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let reference = reference
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Object not found"))?;
        let mut result = ReplicaVerification {
            healthy: 0,
            missing: 0,
            divergent: 0,
        };
        for node in placements {
            let path = node.root.join("objects").join(key);
            match fs::symlink_metadata(&path) {
                Ok(meta) if meta.file_type().is_file() => {
                    if path == reference || same_bytes(&path, &reference)? {
                        result.healthy += 1;
                    } else {
                        result.divergent += 1;
                    }
                }
                Ok(_) => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Replica is not a regular file",
                    ));
                }
                Err(error) if error.kind() == ErrorKind::NotFound => result.missing += 1,
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_storage::StorageNode;

    #[test]
    fn byte_comparison_checks_lengths_and_payloads() {
        let root = tempfile::tempdir().unwrap();
        let left = root.path().join("left");
        let right = root.path().join("right");
        fs::write(&left, b"same").unwrap();
        fs::write(&right, b"same").unwrap();
        assert!(same_bytes(&left, &right).unwrap());
        fs::write(&right, b"diff").unwrap();
        assert!(!same_bytes(&left, &right).unwrap());
        fs::write(&right, b"short").unwrap();
        assert!(!same_bytes(&left, &right).unwrap());
    }

    #[test]
    fn identifies_missing_and_different_copies() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b", "c"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 3).unwrap();
        store.put("part.parquet", b"same").unwrap();
        let assigned = store.placements("part.parquet").unwrap();
        fs::remove_file(assigned[1].root.join("objects/part.parquet")).unwrap();
        fs::write(assigned[2].root.join("objects/part.parquet"), b"other").unwrap();
        let result = store.verify("part.parquet").unwrap();
        assert_eq!(result.healthy, 1);
        assert_eq!(result.missing, 1);
        assert_eq!(result.divergent, 1);
    }

    #[test]
    fn checks_large_equal_and_same_length_divergent_replicas() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let bytes = vec![19_u8; 2 * 1024 * 1024 + 7];
        store.put("large.parquet", &bytes).unwrap();
        assert_eq!(store.verify("large.parquet").unwrap().healthy, 2);
        let replica = store.placements("large.parquet").unwrap()[1];
        let mut different = bytes;
        different[1024 * 1024] ^= 1;
        fs::write(replica.root.join("objects/large.parquet"), different).unwrap();
        let result = store.verify("large.parquet").unwrap();
        assert_eq!((result.healthy, result.divergent), (1, 1));
    }
}
