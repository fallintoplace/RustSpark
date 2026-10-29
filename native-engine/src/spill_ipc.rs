//! Temporary Arrow IPC spill files for memory-bounded native operators.
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

use crate::{arrow_bridge, batch::Batch, schema::Schema, Error, Result};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct SpillFile {
    path: PathBuf,
}

impl SpillFile {
    pub fn path(&self) -> &Path { &self.path }

    pub fn read(&self) -> Result<SpillReader> {
        let file = File::open(&self.path)?;
        let inner = FileReader::try_new(file, None)
            .map_err(|e| Error::Execution(format!("open Arrow spill: {e}")))?;
        Ok(SpillReader { inner, failed: false })
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.path); }
}

pub struct SpillReader {
    inner: FileReader<File>,
    failed: bool,
}

impl Iterator for SpillReader {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed { return None; }
        let result = self.inner.next().map(|record| {
            let record = record
                .map_err(|e| Error::Execution(format!("read Arrow spill: {e}")))?;
            arrow_bridge::from_arrow(&record)
        });
        if result.as_ref().is_some_and(Result::is_err) {
            self.failed = true;
        }
        result
    }
}

/// Own a temporary spill file until its last reader finishes.
pub fn spill(dir: impl AsRef<Path>, schema: &Schema, batches: &[Batch]) -> Result<SpillFile> {
    let identifier = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Io(e.to_string()))?.as_nanos();
    let path = dir.as_ref().join(format!(
        "rustspark-spill-{}-{timestamp}-{identifier}.arrow",
        std::process::id()
    ));
    let output = OpenOptions::new().write(true).create_new(true).open(&path)?;
    let temporary = SpillFile { path };
    let arrow_schema = arrow_bridge::to_arrow(&Batch::empty(schema.clone()))?.schema();
    let mut writer = FileWriter::try_new(output, &arrow_schema)
        .map_err(|e| Error::Execution(format!("create Arrow spill: {e}")))?;
    for batch in batches {
        if batch.schema() != schema {
            return Err(Error::Schema("spill batch schema mismatch".into()));
        }
        let record = arrow_bridge::to_arrow(batch)?;
        writer.write(&record)
            .map_err(|e| Error::Execution(format!("write Arrow spill: {e}")))?;
    }
    writer.finish().map_err(|e| Error::Execution(format!("finish Arrow spill: {e}")))?;
    Ok(temporary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn spill_round_trips_batches_and_cleans_up_file() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(3)]]).unwrap();
        let temp = spill(std::env::temp_dir(), &schema, &[a.clone(), b.clone()]).unwrap();
        assert!(temp.path().is_file());
        assert_eq!(temp.read().unwrap().collect::<Result<Vec<_>>>().unwrap(), vec![a, b]);
        let path = temp.path().to_path_buf();
        drop(temp);
        assert!(!path.exists());
    }
}

#[cfg(test)]
mod malformed_ipc_reader_tests {
    use super::*;
    use std::sync::Arc;
    use arrow_array::{ArrayRef, RecordBatch, UInt64Array};

    #[test]
    fn unrepresentable_arrow_integer_terminates_spill_iteration() {
        let path = std::env::temp_dir().join(format!(
            "native-invalid-arrow-spill-{}-{}.arrow", std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let array = Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef;
        let record = RecordBatch::try_from_iter(vec![("n", array)]).unwrap();
        let mut writer = FileWriter::try_new(File::create(&path).unwrap(), &record.schema()).unwrap();
        writer.write(&record).unwrap();
        writer.finish().unwrap();
        let file = SpillFile { path };
        let mut reader = file.read().unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }
}
