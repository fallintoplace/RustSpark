//! Append-only immutable Parquet parts for a local native analytical store.
//! Publication is per part; no SQL transaction or multi-writer catalog is implied.
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use parquet::arrow::ArrowWriter;
use crate::{arrow_bridge, batch::Batch, Error, Result};

static SEGMENT_ID: AtomicU64 = AtomicU64::new(0);
const MAX_SEGMENT_ROWS: usize = 65_536;

#[derive(Clone, Debug)]
pub struct LocalColumnStore {
    root: PathBuf,
}

struct IncompleteSegment(PathBuf);

impl Drop for IncompleteSegment {
    fn drop(&mut self) { let _ = fs::remove_file(&self.0); }
}

fn table_name(name: &str) -> Result<&str> {
    if name.is_empty() ||
        !name.as_bytes().first().is_some_and(|byte|
            byte.is_ascii_alphabetic() || *byte == b'_') ||
        !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        return Err(Error::Schema("invalid local table identifier".into()));
    }
    Ok(name)
}

impl LocalColumnStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self { root: root.as_ref().to_path_buf() })
    }

    /// Publish one complete, immutable Parquet part without overwriting an
    /// existing part. Caller owns the source batch and its memory budget.
    pub fn append_part(&self, table: &str, batch: &Batch) -> Result<PathBuf> {
        let table = table_name(table)?;
        if batch.schema().is_empty() || batch.num_rows() > MAX_SEGMENT_ROWS {
            return Err(Error::Execution("Parquet part requires columns and at most 65536 rows".into()));
        }
        let record = arrow_bridge::to_arrow(batch)?;
        let directory = self.root.join(table);
        fs::create_dir_all(&directory)?;
        let id = SEGMENT_ID.fetch_add(1, Ordering::Relaxed);
        let tick = SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|error| Error::Io(error.to_string()))?.as_nanos();
        let name = format!("part-{}-{tick}-{id}", std::process::id());
        let destination = directory.join(format!("{name}.parquet"));
        let staged = IncompleteSegment(directory.join(format!("{name}.partial")));
        let file = OpenOptions::new().write(true).create_new(true).open(&staged.0)?;
        let mut writer = ArrowWriter::try_new(file, record.schema(), None)
            .map_err(|error| Error::Execution(format!("create Parquet part: {error}")))?;
        writer.write(&record)
            .map_err(|error| Error::Execution(format!("write Parquet part: {error}")))?;
        writer.close()
            .map_err(|error| Error::Execution(format!("finish Parquet part: {error}")))?;
        drop(writer);
        File::open(&staged.0)?.sync_all()?;
        // hard_link fails if the destination already exists, unlike rename
        // on platforms where rename would silently replace an immutable part.
        fs::hard_link(&staged.0, &destination)?;
        drop(staged);
        #[cfg(unix)]
        File::open(&directory)?.sync_all()?;
        Ok(destination)
    }

    /// Return only fully published Parquet parts, in deterministic order.
    /// Orphaned .partial scratch files are never visible to queries.
    pub fn list_parts(&self, table: &str) -> Result<Vec<PathBuf>> {
        let directory = self.root.join(table_name(table)?);
        if !directory.exists() { return Ok(Vec::new()); }
        let mut paths = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file() &&
                entry.path().extension().is_some_and(|ext| ext == "parquet") {
                paths.push(entry.path());
            }
        }
        paths.sort();
        Ok(paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn immutable_parts_survive_reopen_without_exposing_partial_files() {
        let directory = std::env::temp_dir().join(format!(
            "native-immutable-store-{}-{}", std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        let store = LocalColumnStore::open(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Integer(42)]]).unwrap();
        let path = store.append_part("events", &batch).unwrap();
        assert!(path.exists());
        let parts = LocalColumnStore::open(&directory).unwrap().list_parts("events").unwrap();
        assert_eq!(parts, vec![path.clone()]);
        let decoded = crate::parquet_scan::scan(&path, 8).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(decoded, vec![batch]);
        fs::write(directory.join("events").join("orphan.partial"), b"incomplete").unwrap();
        assert_eq!(store.list_parts("events").unwrap(), vec![path]);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_path_escape_and_unbounded_segment_sizes() {
        let store = LocalColumnStore::open(std::env::temp_dir()).unwrap();
        for name in ["", "..", "../outside", "a/b", "/absolute", "9wrong"] {
            assert!(store.list_parts(name).is_err());
        }
    }
}
