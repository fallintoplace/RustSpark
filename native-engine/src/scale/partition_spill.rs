//! Disk-backed hash exchange for columnar batch iterators.
use std::{
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};
use super::{cancel::QueryControl, chunks::BatchChunks, spill::SpillWriter};

static PARTITION_ID: AtomicU64 = AtomicU64::new(1);

pub struct PartitionFiles {
    paths: Vec<PathBuf>,
    schema: Schema,
}

impl PartitionFiles {
    pub fn paths(&self) -> &[PathBuf] { &self.paths }
    pub fn schema(&self) -> &Schema { &self.schema }
}

impl Drop for PartitionFiles {
    fn drop(&mut self) {
        for path in &self.paths { let _ = std::fs::remove_file(path); }
    }
}

pub struct PartitionWriter {
    owned: Option<PartitionFiles>,
    writers: Option<Vec<SpillWriter<BufWriter<File>>>>,
    key_columns: Vec<usize>,
    batch_rows: usize,
}

fn bucket(row: &[Value], keys: &[usize], count: usize) -> usize {
    let mut hash = DefaultHasher::new();
    for &column in keys { row[column].hash(&mut hash); }
    (hash.finish() as usize) % count
}

impl PartitionWriter {
    pub fn new(
        directory: &Path, schema: Schema, keys: &[&str],
        count: usize, batch_rows: usize
    ) -> Result<Self> {
        if count == 0 || count > 4096 || batch_rows == 0 || keys.is_empty() {
            return Err(Error::Execution("invalid hash partition count, keys or batch size".into()));
        }
        let key_columns = keys.iter().map(|name| schema.column(name))
            .collect::<Result<Vec<_>>>()?;
        let mut owned = PartitionFiles { paths: Vec::new(), schema: schema.clone() };
        let mut writers = Vec::with_capacity(count);
        for _ in 0..count {
            let id = PARTITION_ID.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!("rustspark-hash-{}-{id}.rspill", std::process::id()));
            let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
            owned.paths.push(path);
            writers.push(SpillWriter::new(BufWriter::new(file), schema.clone())?);
        }
        Ok(Self { owned: Some(owned), writers: Some(writers), key_columns, batch_rows })
    }

    pub fn write_batch(&mut self, batch: &Batch, control: &QueryControl) -> Result<()> {
        let schema = &self.owned.as_ref().ok_or_else(|| Error::Execution("writer finalized".into()))?.schema;
        if batch.schema() != schema { return Err(Error::Schema("partition schema mismatch".into())); }
        let writers = self.writers.as_mut().ok_or_else(|| Error::Execution("writer finalized".into()))?;
        for part in BatchChunks::new(batch, self.batch_rows)? {
            control.check()?;
            let part = part?;
            let mut groups = vec![Vec::new(); writers.len()];
            for row in part.rows()? {
                let index = bucket(&row, &self.key_columns, writers.len());
                groups[index].push(row);
            }
            for (writer, rows) in writers.iter_mut().zip(groups) {
                if !rows.is_empty() { writer.write_batch(&Batch::from_rows(schema.clone(), rows)?)?; }
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<PartitionFiles> {
        for writer in self.writers.take().unwrap_or_default() { writer.finish()?; }
        Ok(self.owned.take().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::BufReader};
    use super::super::spill::SpillReader;
    use crate::{scalar::Value, schema::{DataType, Field}};
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn identical_keys_reach_the_same_shard() {
        let dir = std::env::temp_dir().join(format!("native-shards-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
            vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let mut writer = PartitionWriter::new(&dir, schema, &["id"], 8, 2).unwrap();
        writer.write_batch(&batch, &QueryControl::default()).unwrap();
        let files = writer.finish().unwrap();
        let mut seen = 0;
        for file in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(file).unwrap()), 2).unwrap();
            let mut ids = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() {
                ids.extend(batch.rows().unwrap().into_iter().map(|row| row[0].clone()));
            }
            assert_ne!(ids.iter().filter(|x| **x == Value::Integer(1)).count(), 1);
            seen += ids.len();
        }
        assert_eq!(seen, 4);
        drop(files);
        fs::remove_dir(dir).unwrap();
    }
}
