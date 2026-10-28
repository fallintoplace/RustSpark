use std::{
    fs::{self, File},
    io::{self, ErrorKind, Read, Seek, SeekFrom},
};

use super::ReplicaStore;

const MAX_RANGE_BYTES: usize = 64 * 1024 * 1024;

impl ReplicaStore {
    /// Read an exact byte range without loading the whole object.
    pub fn read_range(&self, key: &str, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        if length > MAX_RANGE_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Range exceeds the 64 MiB read limit",
            ));
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "Range overflow"))?;
        let mut last_error = None;
        // A failed replica can have read part of the requested range. Reuse
        // this allocation rather than allocating up to 64 MiB per retry.
        let mut bytes = Vec::new();
        for node in self.placements(key)? {
            let path = node.root.join("objects").join(key);
            let attempt = (|| {
                if !fs::symlink_metadata(&path)?.file_type().is_file() {
                    return Err(io::Error::new(ErrorKind::InvalidData, "Unsafe replica"));
                }
                let mut file = File::open(path)?;
                if end > file.metadata()?.len() {
                    return Err(io::Error::new(ErrorKind::UnexpectedEof, "Range exceeds object"));
                }
                if length > 0 {
                    file.seek(SeekFrom::Start(offset))?;
                    bytes.resize(length, 0);
                    file.read_exact(&mut bytes)?;
                }
                Ok(())
            })();
            match attempt {
                Ok(()) => return Ok(bytes),
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

    #[test]
    fn returns_exact_ranges_and_rejects_out_of_bounds() {
        let root = tempfile::tempdir().unwrap();
        let store = ReplicaStore::new(
            vec![StorageNode::new("a", root.path()).unwrap()],
            1,
        )
        .unwrap();
        store.put("part.parquet", b"abcdefgh").unwrap();
        assert_eq!(store.read_range("part.parquet", 2, 3).unwrap(), b"cde");
        assert_eq!(store.read_range("part.parquet", 8, 0).unwrap(), b"");
        assert!(store.read_range("part.parquet", 7, 2).is_err());
        assert!(store.read_range("part.parquet", u64::MAX, 2).is_err());
    }

    #[test]
    fn range_failover_keeps_exact_bytes_after_short_primary() {
        let root = tempfile::tempdir().unwrap();
        let nodes = ["a", "b"].map(|id| {
            let path = root.path().join(id);
            fs::create_dir(&path).unwrap();
            StorageNode::new(id, path).unwrap()
        });
        let store = ReplicaStore::new(nodes.to_vec(), 2).unwrap();
        let data = vec![73_u8; 1024 * 1024];
        store.put("large.parquet", &data).unwrap();
        let first = store.placements("large.parquet").unwrap()[0];
        fs::write(first.root.join("objects/large.parquet"), b"too short").unwrap();
        assert_eq!(store.read_range("large.parquet", 9000, 4096).unwrap(), vec![73; 4096]);
        assert_eq!(store.read_range("large.parquet", data.len() as u64, 0).unwrap(), b"");
    }
}
