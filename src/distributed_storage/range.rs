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
                file.seek(SeekFrom::Start(offset))?;
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes)?;
                Ok(bytes)
            })();
            match attempt {
                Ok(bytes) => return Ok(bytes),
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
}
