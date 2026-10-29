//! Disk-backed hash exchange for columnar batch iterators.
use std::{
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, schema::Schema, Error, Result};
use super::{cancel::QueryControl, spill::SpillWriter};

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
    writers: Option<Vec<Option<SpillWriter<BufWriter<File>>>>>,
    key_columns: Vec<usize>,
    batch_rows: usize,
}

fn bucket(batch: &Batch, row: usize, keys: &[usize], count: usize) -> Result<usize> {
    let mut hash = crate::hash_partition::PartitionHasher::default();
    for &column in keys { batch.value(row, column)?.hash(&mut hash); }
    let hashed = hash.finish();
    Ok(if count.is_power_of_two() {
        (hashed as usize) & (count - 1)
    } else {
        (hashed % count as u64) as usize
    })
}

fn write_shard(
    writers: &mut [Option<SpillWriter<BufWriter<File>>>],
    paths: &[PathBuf], schema: &Schema, index: usize,
    batch: &Batch, rows: &[usize],
) -> Result<()> {
    let slot = &mut writers[index];
    if slot.is_none() {
        let file = OpenOptions::new().write(true).create_new(true).open(&paths[index])?;
        *slot = Some(SpillWriter::new(BufWriter::new(file), schema.clone())?);
    }
    slot.as_mut().expect("shard writer initialized").write_batch(&batch.take(rows)?)
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
        let mut owned = PartitionFiles { paths: Vec::with_capacity(count), schema };
        for _ in 0..count {
            let id = PARTITION_ID.fetch_add(1, Ordering::Relaxed);
            owned.paths.push(directory.join(format!(
                "rustspark-hash-{}-{id}.rspill", std::process::id(),
            )));
        }
        // Only touched shards need file handles while writing. Empty shards
        // receive a valid schema-only spill file when finish() is called.
        let writers = std::iter::repeat_with(|| None).take(count).collect();
        Ok(Self { owned: Some(owned), writers: Some(writers), key_columns, batch_rows })
    }

    pub fn write_batch(&mut self, batch: &Batch, control: &QueryControl) -> Result<()> {
        let owned = self.owned.as_ref().ok_or_else(|| Error::Execution("writer finalized".into()))?;
        if batch.schema() != &owned.schema { return Err(Error::Schema("partition schema mismatch".into())); }
        let writers = self.writers.as_mut().ok_or_else(|| Error::Execution("writer finalized".into()))?;
        let mut dense: Option<Vec<Vec<usize>>> = None;
        let mut start = 0;
        while start < batch.num_rows() {
            control.check()?;
            let end = start.saturating_add(self.batch_rows).min(batch.num_rows());
            if writers.len() > (end - start).saturating_mul(4) {
                let mut sparse = std::collections::HashMap::<usize, Vec<usize>>::new();
                for row in start..end {
                    let index = bucket(batch, row, &self.key_columns, writers.len())?;
                    sparse.entry(index).or_default().push(row);
                }
                for (index, rows) in sparse {
                    write_shard(writers, &owned.paths, &owned.schema, index, batch, &rows)?;
                }
            } else {
                let groups = dense.get_or_insert_with(|| vec![Vec::new(); writers.len()]);
                for group in groups.iter_mut() { group.clear(); }
                for row in start..end {
                    let index = bucket(batch, row, &self.key_columns, writers.len())?;
                    groups[index].push(row);
                }
                for (index, rows) in groups.iter().enumerate() {
                    if !rows.is_empty() {
                        write_shard(writers, &owned.paths, &owned.schema, index, batch, rows)?;
                    }
                }
            }
            start = end;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<PartitionFiles> {
        let owned = self.owned.take().expect("active partition writer");
        for (index, writer) in self.writers.take().unwrap_or_default().into_iter().enumerate() {
            if let Some(writer) = writer {
                writer.finish()?;
            } else {
                // Readers expect one schema-checked file per partition,
                // including untouched partitions.
                let file = OpenOptions::new().write(true).create_new(true)
                    .open(&owned.paths[index])?;
                SpillWriter::new(BufWriter::new(file), owned.schema.clone())?.finish()?;
            }
        }
        Ok(owned)
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
        let expected = crate::hash_partition::partition(&batch, &["id"], 8).unwrap();
        let mut seen = 0;
        for (index, file) in files.paths().iter().enumerate() {
            let mut reader = SpillReader::new(BufReader::new(File::open(file).unwrap()), 2).unwrap();
            let mut ids = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() {
                ids.extend(batch.rows().unwrap().into_iter().map(|row| row[0].clone()));
            }
            assert_ne!(ids.iter().filter(|x| **x == Value::Integer(1)).count(), 1);
            assert_eq!(ids.as_slice(), expected[index].column(0).unwrap(), "in-memory and spilled hash partitions must agree");
            seen += ids.len();
        }
        assert_eq!(seen, 4);
        drop(files);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod consolidated_partition_offset_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    #[test]
    fn partition_hashes_keys_without_materializing_rows() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let source = Batch::from_rows(schema, vec![
            vec![Value::Integer(2), Value::Text("wide A".repeat(100))],
            vec![Value::Null, Value::Text("wide B".repeat(100))],
            vec![Value::Integer(2), Value::Text("wide C".repeat(100))],
        ]).unwrap();
        assert_eq!(bucket(&source, 0, &[0], 8).unwrap(), bucket(&source, 2, &[0], 8).unwrap());
        assert!(bucket(&source, 3, &[0], 8).is_err());
        assert!(bucket(&source, 0, &[2], 8).is_err());
    }
}

#[cfg(test)]
mod reused_partition_bucket_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}, scale::spill::SpillReader};
    use std::{fs, io::BufReader};

    #[test]
    fn many_small_chunks_keep_all_rows_in_original_bucket_order() {
        let directory = std::env::temp_dir().join(format!(
            "reused-buckets-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..120).map(|n|
            vec![if n % 7 == 0 { Value::Null } else { Value::Integer(n % 13) }]
        ).collect()).unwrap();
        let mut writer = PartitionWriter::new(&directory, schema, &["k"], 16, 3).unwrap();
        writer.write_batch(&batch, &QueryControl::default()).unwrap();
        let files = writer.finish().unwrap();
        let expected = crate::hash_partition::partition(&batch, &["k"], 16).unwrap();
        for (index, path) in files.paths().iter().enumerate() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 3).unwrap();
            let mut actual = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() {
                actual.extend(batch.rows().unwrap());
            }
            assert_eq!(actual, expected[index].rows().unwrap());
        }
        drop(files);
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod sparse_partition_writer_tests {
    use super::*;
    use super::super::spill::SpillReader;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::{fs, io::BufReader, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn sparse_writer_preserves_per_shard_row_order() {
        let dir = std::env::temp_dir().join(format!("native-sparse-part-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let data = Batch::from_rows(schema.clone(), (0..29).map(|n| vec![
            if n % 5 == 0 { Value::Null } else { Value::Integer(n % 7) }
        ]).collect()).unwrap();
        let mut writer = PartitionWriter::new(&dir, schema, &["key"], 64, 4).unwrap();
        writer.write_batch(&data, &QueryControl::default()).unwrap();
        let files = writer.finish().unwrap();
        let expected = crate::hash_partition::partition(&data, &["key"], 64).unwrap();
        for (file, wanted) in files.paths().iter().zip(&expected) {
            let mut reader = SpillReader::new(BufReader::new(File::open(file).unwrap()), 4).unwrap();
            let mut rows = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() { rows.extend(batch.rows().unwrap()); }
            assert_eq!(rows, wanted.rows().unwrap());
        }
        drop(files);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod masked_spill_buckets_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    #[test]
    fn bucket_choice_matches_remainder_for_power_of_two_and_odd_counts() {
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, (0..500).map(|n| vec![
            if n % 13 == 0 { Value::Null } else { Value::Integer(n % 89) }
        ]).collect()).unwrap();
        for partitions in [1, 2, 4, 16, 64, 127, 256, 1024, 4096] {
            for row in 0..batch.num_rows() {
                let mut hasher = crate::hash_partition::PartitionHasher::default();
                batch.value(row, 0).unwrap().hash(&mut hasher);
                assert_eq!(bucket(&batch, row, &[0], partitions).unwrap(),
                    (hasher.finish() % partitions as u64) as usize);
            }
        }
    }
}

#[cfg(test)]
mod lazy_partition_writer_tests {
    use super::*;
    use super::super::spill::SpillReader;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::{fs, io::BufReader, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn only_touched_shards_open_before_finish_and_empty_shards_still_read() {
        let dir = std::env::temp_dir().join(format!("native-lazy-shard-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..9).map(|_| vec![Value::Integer(3)]).collect()).unwrap();
        let mut writer = PartitionWriter::new(&dir, schema, &["k"], 64, 3).unwrap();
        let paths = writer.owned.as_ref().unwrap().paths().to_vec();
        assert!(paths.iter().all(|p| !p.exists()));
        writer.write_batch(&batch, &QueryControl::default()).unwrap();
        assert_eq!(paths.iter().filter(|p| p.exists()).count(), 1);
        let files = writer.finish().unwrap();
        assert!(files.paths().iter().all(|p| p.exists()));
        let mut observed = Vec::new();
        for path in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 3).unwrap();
            while let Some(part) = reader.next_batch().unwrap() { observed.extend(part.rows().unwrap()); }
        }
        assert_eq!(observed, batch.rows().unwrap());
        drop(files);
        fs::remove_dir(dir).unwrap();
    }
}
