//! Exact NULL-safe DISTINCT using disk partitions and bounded local hash sets.
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};
use super::{
    cancel::QueryControl,
    partition_spill::PartitionWriter, spill::{SpillReader, SpillWriter}
};

static DISTINCT_ID: AtomicU64 = AtomicU64::new(1);

pub struct DistinctFiles {
    paths: Vec<PathBuf>,
    schema: Schema,
}

impl DistinctFiles {
    pub fn paths(&self) -> &[PathBuf] { &self.paths }
    pub fn schema(&self) -> &Schema { &self.schema }
}

impl Drop for DistinctFiles {
    fn drop(&mut self) {
        for path in &self.paths { let _ = std::fs::remove_file(path); }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn distinct_to_disk<I>(
    input: I, schema: Schema, directory: &Path, partitions: usize,
    max_unique_rows_per_partition: usize, io_batch_rows: usize,
    control: &QueryControl,
) -> Result<DistinctFiles>
where I: IntoIterator<Item = Result<Batch>>
{
    if schema.is_empty() {
        return Err(Error::Unsupported("DISTINCT requires at least one column".into()));
    }
    if max_unique_rows_per_partition == 0 || io_batch_rows == 0 {
        return Err(Error::Execution("invalid DISTINCT state or IO limit".into()));
    }
    let keys = schema.fields().iter()
        .map(|field| field.name.as_str()).collect::<Vec<_>>();
    let mut writer = PartitionWriter::new(
        directory, schema.clone(), &keys, partitions, io_batch_rows)?;
    for incoming in input {
        control.check()?;
        writer.write_batch(&incoming?, control)?;
    }
    let input_files = writer.finish()?;
    let mut result = DistinctFiles { paths: Vec::new(), schema: schema.clone() };
    for shard in input_files.paths() {
        control.check()?;
        let mut reader = SpillReader::new(BufReader::new(File::open(shard)?), io_batch_rows)?;
        if reader.schema() != &schema {
            return Err(Error::Schema("DISTINCT partition schema mismatch".into()));
        }
        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        while let Some(batch) = reader.next_batch()? {
            control.check()?;
            for row in batch.rows()? {
                if !seen.contains(&row) {
                    if seen.len() >= max_unique_rows_per_partition {
                        return Err(Error::Execution("DISTINCT partition exceeds state budget".into()));
                    }
                    seen.insert(row);
                }
            }
        }
        let id = DISTINCT_ID.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("rustspark-distinct-{}-{id}.rspill", std::process::id()));
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        result.paths.push(path);
        let mut output = SpillWriter::new(BufWriter::new(file), schema.clone())?;
        let mut pending = Vec::with_capacity(io_batch_rows.min(seen.len()));
        for row in seen {
            pending.push(row);
            if pending.len() == io_batch_rows {
                output.write_batch(&Batch::from_rows(schema.clone(), std::mem::take(&mut pending))?)?;
            }
        }
        if !pending.is_empty() {
            output.write_batch(&Batch::from_rows(schema.clone(), pending)?)?;
        }
        output.finish()?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field}};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn deduplicates_duplicate_and_null_rows_across_batches() {
        let dir = std::env::temp_dir().join(format!("native-distinct-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let one = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(4)], vec![Value::Null]
        ]).unwrap();
        let two = Batch::from_rows(schema.clone(), vec![
            vec![Value::Null], vec![Value::Integer(4)], vec![Value::Integer(5)]
        ]).unwrap();
        let files = distinct_to_disk([Ok(one), Ok(two)], schema, &dir, 8, 10, 2,
            &QueryControl::default()).unwrap();
        let mut values = Vec::new();
        for path in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                values.extend(batch.rows().unwrap().into_iter().map(|row| row[0].clone()));
            }
        }
        assert_eq!(values.len(), 3);
        assert!(values.contains(&Value::Null));
        assert!(values.contains(&Value::Integer(4)));
        assert!(values.contains(&Value::Integer(5)));
        drop(files);
        fs::remove_dir(dir).unwrap();
    }
}
