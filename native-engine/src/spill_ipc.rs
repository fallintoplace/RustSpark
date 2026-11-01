//! Temporary Arrow IPC spill files for memory-bounded native operators.
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{SystemTime, UNIX_EPOCH},
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

use crate::{arrow_bridge, batch::Batch, schema::Schema, Error, Result};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct SpillPath(PathBuf);

impl Drop for SpillPath {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
}

#[derive(Debug)]
pub struct SpillFile {
    owner: Arc<SpillPath>,
}

impl SpillFile {
    pub fn path(&self) -> &Path { &self.owner.0 }

    pub fn read(&self) -> Result<SpillReader> {
        let file = File::open(self.path())?;
        let inner = FileReader::try_new(file, None)
            .map_err(|e| Error::Execution(format!("open Arrow spill: {e}")))?;
        Ok(SpillReader { inner, failed: false, _owner: Arc::clone(&self.owner) })
    }
}

pub struct SpillReader {
    // Drop file handle before removing scratch file
    inner: FileReader<File>,
    failed: bool,
    _owner: Arc<SpillPath>,
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

fn start_spill(
    dir: impl AsRef<Path>, schema: &Schema,
) -> Result<(SpillFile, FileWriter<File>)> {
    let identifier = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Io(e.to_string()))?.as_nanos();
    let path = dir.as_ref().join(format!(
        "rustspark-spill-{}-{timestamp}-{identifier}.arrow",
        std::process::id()
    ));
    let output = crate::scale::private_files::create_private(&path)?;
    // Establish ownership before any fallible Arrow construction. Both a
    // schema conversion error and a late write error remove the scratch file.
    let temporary = SpillFile { owner: Arc::new(SpillPath(path)) };
    let arrow_schema = arrow_bridge::to_arrow(&Batch::empty(schema.clone()))?.schema();
    let writer = FileWriter::try_new(output, &arrow_schema)
        .map_err(|e| Error::Execution(format!("create Arrow spill: {e}")))?;
    Ok((temporary, writer))
}

fn write_spill_batch(
    writer: &mut FileWriter<File>, schema: &Schema, batch: &Batch,
) -> Result<()> {
    if batch.schema() != schema {
        return Err(Error::Schema("spill batch schema mismatch".into()));
    }
    let record = arrow_bridge::to_arrow(batch)?;
    writer.write(&record)
        .map_err(|e| Error::Execution(format!("write Arrow spill: {e}")))
}

/// Own a temporary spill file until its last reader finishes.
pub fn spill(dir: impl AsRef<Path>, schema: &Schema, batches: &[Batch]) -> Result<SpillFile> {
    let (temporary, mut writer) = start_spill(dir, schema)?;
    for batch in batches {
        write_spill_batch(&mut writer, schema, batch)?;
    }
    writer.finish().map_err(|e| Error::Execution(format!("finish Arrow spill: {e}")))?;
    Ok(temporary)
}

/// Write one owned native batch at a time without retaining an input slice.
/// The iterator can produce batches lazily, and failed inputs clean up the
/// partially written temporary file before reporting the error.
pub fn spill_stream(
    dir: impl AsRef<Path>, schema: &Schema,
    batches: impl IntoIterator<Item = Result<Batch>>,
) -> Result<SpillFile> {
    let (temporary, mut writer) = start_spill(dir, schema)?;
    for next in batches {
        let batch = next?;
        write_spill_batch(&mut writer, schema, &batch)?;
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
        let file = SpillFile { owner: Arc::new(SpillPath(path)) };
        let mut reader = file.read().unwrap();
        assert!(reader.next().unwrap().is_err());
        assert!(reader.next().is_none());
    }
}
#[cfg(test)]
mod spill_reader_ownership_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn open_reader_keeps_scratch_file_alive_after_owner_drop() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(11)], vec![Value::Integer(12)],
        ]).unwrap();
        let file = spill(std::env::temp_dir(), &schema, &[batch.clone()]).unwrap();
        let path = file.path().to_owned();
        let mut reader = file.read().unwrap();
        drop(file);
        assert!(path.exists(), "reader must retain the spill file");
        assert_eq!(reader.next().unwrap().unwrap(), batch);
        assert!(reader.next().is_none());
        drop(reader);
        assert!(!path.exists(), "last reader must remove the scratch file");
    }
}

#[cfg(test)]
mod streaming_ipc_spill_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as TestOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);

    fn directory() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-stream-ipc-{}-{}",
            std::process::id(), ID.fetch_add(1, TestOrdering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        dir
    }

    #[test]
    fn lazy_source_writes_and_reads_many_bounded_arrow_batches() {
        let dir = directory();
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let input = (0..512).map(|row| {
            Batch::from_rows(schema.clone(), vec![vec![
                if row % 17 == 0 { Value::Null }
                else { Value::Text(format!("row {row} 🐈")) }
            ]])
        });
        let file = spill_stream(&dir, &schema, input).unwrap();
        let path = file.path().to_owned();
        let mut count = 0usize;
        for batch in file.read().unwrap() {
            let batch = batch.unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.value(0, 0).unwrap(),
                &if count % 17 == 0 { Value::Null }
                else { Value::Text(format!("row {count} 🐈")) });
            count += 1;
        }
        assert_eq!(count, 512);
        drop(file);
        assert!(!path.exists());
        fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn late_producer_and_schema_errors_remove_partial_files() {
        let dir = directory();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let valid = Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(1)]]).unwrap();
        let source = vec![
            Ok(valid.clone()), Err(Error::Execution("upstream failed".into())),
        ];
        assert!(spill_stream(&dir, &schema, source).is_err());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);

        let invalid_schema = Schema::new(vec![Field::new("other", DataType::Text, false)])
            .unwrap();
        let invalid = Batch::from_rows(invalid_schema,
            vec![vec![Value::Text("wrong".into())]]).unwrap();
        assert!(spill_stream(&dir, &schema,
            [Ok(valid), Ok(invalid)]).is_err());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(dir).unwrap();
    }
}
