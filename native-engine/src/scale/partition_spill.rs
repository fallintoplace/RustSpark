//! Disk-backed hash exchange for columnar batch iterators.
use std::{
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};
use super::{cancel::QueryControl, spill::SpillWriter};

static PARTITION_ID: AtomicU64 = AtomicU64::new(1);
const MAX_OPEN_SHARDS: usize = 64;

// A shard has a stable index, so its LRU links can be updated without
// searching the bounded set of open handles.
struct WriterLru {
    previous: Vec<Option<usize>>,
    next: Vec<Option<usize>>,
    oldest: Option<usize>,
    newest: Option<usize>,
    len: usize,
}
impl WriterLru {
    fn new(capacity: usize) -> Self {
        Self { previous: vec![None; capacity], next: vec![None; capacity],
            oldest: None, newest: None, len: 0 }
    }
    fn len(&self) -> usize { self.len }
    fn unlink(&mut self, index: usize) {
        let previous = self.previous[index];
        let next = self.next[index];
        if let Some(prior) = previous { self.next[prior] = next; }
        else { self.oldest = next; }
        if let Some(following) = next { self.previous[following] = previous; }
        else { self.newest = previous; }
        self.previous[index] = None;
        self.next[index] = None;
    }
    fn push_newest(&mut self, index: usize) {
        self.previous[index] = self.newest;
        self.next[index] = None;
        if let Some(last) = self.newest { self.next[last] = Some(index); }
        else { self.oldest = Some(index); }
        self.newest = Some(index);
    }
    fn insert(&mut self, index: usize) {
        self.push_newest(index);
        self.len += 1;
    }
    fn touch(&mut self, index: usize) {
        if self.newest == Some(index) { return; }
        self.unlink(index);
        self.push_newest(index);
    }
    fn pop_oldest(&mut self) -> Option<usize> {
        let index = self.oldest?;
        self.unlink(index);
        self.len -= 1;
        Some(index)
    }
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = &usize> {
        std::iter::successors(self.oldest.as_ref(), |index| self.next[**index].as_ref())
    }
}

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
    // Close buffered output handles before cleanup removes the owned files.
    writers: Option<Vec<Option<SpillWriter<BufWriter<File>>>>>,
    owned: Option<PartitionFiles>,
    open_order: WriterLru,
    initialized: Vec<bool>,
    key_columns: Vec<usize>,
    batch_rows: usize,
    failed: bool,
}

#[cfg(test)]
fn bucket(batch: &Batch, row: usize, keys: &[usize], count: usize) -> Result<usize> {
    if count == 0 {
        return Err(Error::Execution("partition count must be positive".into()));
    }
    let mut hash = crate::hash_partition::PartitionHasher::default();
    for &column in keys { batch.value(row, column)?.hash(&mut hash); }
    let hashed = hash.finish();
    Ok(if count.is_power_of_two() {
        (hashed as usize) & (count - 1)
    } else {
        (hashed % count as u64) as usize
    })
}

// These column slices are schema-checked once for the whole incoming batch.
fn bucket_bound(columns: &[&[Value]], row: usize, count: usize) -> usize {
    let mut hash = crate::hash_partition::PartitionHasher::default();
    for column in columns { column[row].hash(&mut hash); }
    let hashed = hash.finish();
    if count.is_power_of_two() {
        (hashed as usize) & (count - 1)
    } else {
        (hashed % count as u64) as usize
    }
}

const MAX_SPILL_WIRE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SPILL_CELLS: usize = 4_000_000;

// The spill format stores one tag per cell, eight bytes per number,
// and a four-byte length before each TEXT payload.
fn row_wire_bytes(batch: &Batch, row: usize) -> Result<usize> {
    let mut bytes = batch.schema().len();
    for column in 0..batch.schema().len() {
        let extra = match batch.value(row, column)? {
            Value::Text(text) => text.len().checked_add(4),
            Value::Integer(_) | Value::Float(_) => Some(8),
            Value::Null | Value::Boolean(_) => Some(0),
        }.ok_or_else(|| Error::Execution("partition spill wire byte overflow".into()))?;
        bytes = bytes.checked_add(extra)
            .ok_or_else(|| Error::Execution("partition spill wire byte overflow".into()))?;
    }
    Ok(bytes)
}

/// Visit wire-budgeted row windows. Row-index selection and contiguous
/// ranges share this logic without allocating extra gathered batches.
fn write_bounded_windows(
    batch: &Batch, rows: usize, row_at: impl Fn(usize) -> usize,
    max_bytes: usize, mut emit: impl FnMut(usize, usize) -> Result<()>,
) -> Result<()> {
    let max_rows = MAX_SPILL_CELLS / batch.schema().len().max(1);
    if max_bytes < 4 || max_rows == 0 {
        return Err(Error::Execution("partition spill batch budget too small".into()));
    }
    let mut start = 0usize;
    while start < rows {
        let mut end = start;
        let mut bytes = 4usize;
        while end < rows && end - start < max_rows {
            let next = row_wire_bytes(batch, row_at(end))?;
            if next > max_bytes - 4 {
                return Err(Error::Execution("partition spill row exceeds wire byte limit".into()));
            }
            if next > max_bytes - bytes { break; }
            bytes += next;
            end += 1;
        }
        emit(start, end)?;
        start = end;
    }
    Ok(())
}

// Keep a bounded LRU of open writers. Reopening an initialized shard appends
// batches without emitting a second spill header into the same file.
fn ensure_open(
    writers: &mut [Option<SpillWriter<BufWriter<File>>>],
    order: &mut WriterLru, initialized: &mut [bool],
    paths: &[PathBuf], schema: &Schema, index: usize,
) -> Result<()> {
    if writers[index].is_none() {
        if order.len() == MAX_OPEN_SHARDS {
            let victim = order.pop_oldest().expect("open writer order nonempty");
            writers[victim].take().expect("evicted writer exists").finish()?;
        }
        let writer = if initialized[index] {
            let file = OpenOptions::new().append(true).open(&paths[index])?;
            SpillWriter::resume(BufWriter::new(file), schema.clone())
        } else {
            let file = super::private_files::create_private(&paths[index])?;
            let writer = SpillWriter::new(BufWriter::new(file), schema.clone())?;
            initialized[index] = true;
            writer
        };
        writers[index] = Some(writer);
        order.insert(index);
    } else {
        // Stable indices let us update the hot-shard order in O(1).
        order.touch(index);
    }
    Ok(())
}

fn write_shard(
    writers: &mut [Option<SpillWriter<BufWriter<File>>>],
    order: &mut WriterLru, initialized: &mut [bool],
    paths: &[PathBuf], schema: &Schema, index: usize,
    batch: &Batch, rows: &[usize],
) -> Result<()> {
    ensure_open(writers, order, initialized, paths, schema, index)?;
    let writer = writers[index].as_mut().expect("shard writer initialized");
    write_bounded_windows(batch, rows.len(), |position| rows[position],
        MAX_SPILL_WIRE_BYTES, |start, end|
            writer.write_batch_indices(batch, &rows[start..end]))
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
        Ok(Self {
            writers: Some(writers), owned: Some(owned),
            open_order: WriterLru::new(count), initialized: vec![false; count],
            key_columns, batch_rows, failed: false,
        })
    }

    pub fn write_batch(&mut self, batch: &Batch, control: &QueryControl) -> Result<()> {
        if self.failed {
            return Err(Error::Execution("partition writer poisoned by earlier failure".into()));
        }
        // A late shard error may follow previously persisted shard windows.
        // Never allow finish() to publish a partial partition exchange.
        let result = self.write_batch_inner(batch, control);
        if result.is_err() { self.failed = true; }
        result
    }

    fn write_batch_inner(&mut self, batch: &Batch, control: &QueryControl) -> Result<()> {
        let owned = self.owned.as_ref().ok_or_else(|| Error::Execution("writer finalized".into()))?;
        if batch.schema() != &owned.schema { return Err(Error::Schema("partition schema mismatch".into())); }
        let writers = self.writers.as_mut().ok_or_else(|| Error::Execution("writer finalized".into()))?;
        if writers.len() == 1 && batch.num_rows() != 0 {
            // Every row belongs to the only shard. Avoid per-row hashing,
            // selection indices, and cloning values through Batch::take.
            control.check()?;
            ensure_open(writers, &mut self.open_order, &mut self.initialized,
                &owned.paths, &owned.schema, 0)?;
            let slot = &mut writers[0];
            let mut start = 0;
            while start < batch.num_rows() {
                control.check()?;
                let end = start.saturating_add(self.batch_rows).min(batch.num_rows());
                let writer = slot.as_mut().expect("single shard writer initialized");
                write_bounded_windows(batch, end - start, |offset| start + offset,
                    MAX_SPILL_WIRE_BYTES, |from, to|
                        writer.write_batch_range(batch, start + from, start + to))?;
                start = end;
            }
            return Ok(());
        }
        let key_slices = self.key_columns.iter()
            .map(|&index| batch.column(index)).collect::<Result<Vec<_>>>()?;
        // Preserve the last resolved destination across output windows. Sorted
        // shuffle inputs can contain long runs of the same composite key.
        let shard_count = writers.len();
        let mut previous_row: Option<usize> = None;
        let mut previous_shard = 0usize;
        let mut shard_for_row = |row: usize| {
            let shard = if previous_row.is_some_and(|previous|
                key_slices.iter().all(|key| key[row] == key[previous])) {
                previous_shard
            } else {
                bucket_bound(&key_slices, row, shard_count)
            };
            previous_row = Some(row);
            previous_shard = shard;
            shard
        };
        let mut dense: Option<Vec<Vec<usize>>> = None;
        // Sparse exchanges can span many small batches. Reuse the shard
        // buckets instead of rebuilding a hash table and row vectors for each.
        let mut sparse = std::collections::HashMap::<usize, Vec<usize>>::new();
        let mut touched = Vec::<usize>::new();
        let mut start = 0;
        while start < batch.num_rows() {
            control.check()?;
            let end = start.saturating_add(self.batch_rows).min(batch.num_rows());
            if writers.len() > (end - start).saturating_mul(4) {
                for row in start..end {
                    let index = shard_for_row(row);
                    let rows = sparse.entry(index).or_default();
                    if rows.is_empty() { touched.push(index); }
                    rows.push(row);
                }
                for &index in &touched {
                    let rows = &sparse[&index];
                    write_shard(writers, &mut self.open_order, &mut self.initialized,
                        &owned.paths, &owned.schema, index, batch, rows)?;
                    sparse.get_mut(&index).expect("touched shard exists").clear();
                }
                touched.clear();
            } else {
                let groups = dense.get_or_insert_with(|| vec![Vec::new(); writers.len()]);
                for group in groups.iter_mut() { group.clear(); }
                for row in start..end {
                    let index = shard_for_row(row);
                    groups[index].push(row);
                }
                for (index, rows) in groups.iter().enumerate() {
                    if !rows.is_empty() {
                        write_shard(writers, &mut self.open_order, &mut self.initialized,
                            &owned.paths, &owned.schema, index, batch, rows)?;
                    }
                }
            }
            start = end;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<PartitionFiles> {
        if self.failed {
            return Err(Error::Execution("partition writer poisoned by earlier failure".into()));
        }
        // Keep path ownership until every file is finalized. If a flush or
        // create_new fails, dropping self removes the incomplete spill files.
        let owned = self.owned.as_ref().expect("active partition writer");
        for (index, writer) in self.writers.take().unwrap_or_default().into_iter().enumerate() {
            if let Some(writer) = writer {
                writer.finish()?;
            } else if !self.initialized[index] {
                // Readers expect one schema-checked file per partition,
                // including untouched partitions. Already closed shards exist.
                let file = super::private_files::create_private(&owned.paths[index])?;
                SpillWriter::new(BufWriter::new(file), owned.schema.clone())?.finish()?;
            }
        }
        Ok(self.owned.take().expect("active partition writer"))
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

#[cfg(test)]
mod single_shard_partition_zero_copy_tests {
    use super::*;
    use crate::{
        scalar::Value, schema::{DataType, Field, Schema}, scale::spill::SpillReader,
    };
    use std::{fs, io::BufReader};

    #[test]
    fn one_shard_writes_contiguous_text_windows_without_reordering() {
        let dir = std::env::temp_dir().join(format!("native-single-shard-{}-{}",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let source = Batch::from_rows(schema.clone(), (0..30).map(|n| vec![
            if n % 5 == 0 { Value::Null } else { Value::Integer(n) },
            Value::Text(format!("row-{n}-{}", "é".repeat(1000))),
        ]).collect()).unwrap();
        let mut writer = PartitionWriter::new(&dir, schema, &["key"], 1, 4).unwrap();
        writer.write_batch(&source, &QueryControl::default()).unwrap();
        let files = writer.finish().unwrap();
        let mut reader = SpillReader::new(BufReader::new(
            File::open(&files.paths()[0]).unwrap()), 4).unwrap();
        let mut rows = Vec::new();
        while let Some(part) = reader.next_batch().unwrap() {
            rows.extend(part.rows().unwrap());
        }
        assert_eq!(rows, source.rows().unwrap());
        drop(reader);
        drop(files);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod failed_partition_finish_cleanup_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::fs;

    #[test]
    fn failed_finalization_removes_prior_partial_spill_files() {
        let directory = std::env::temp_dir().join(format!(
            "failed-native-partition-finish-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                .unwrap().as_nanos(),
        ));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let writer = PartitionWriter::new(&directory, schema, &["key"], 2, 16).unwrap();
        let paths = writer.owned.as_ref().unwrap().paths().to_vec();

        // A directory in the second shard slot forces finalization to fail
        // after the first empty shard file has already been created.
        fs::create_dir(&paths[1]).unwrap();
        assert!(writer.finish().is_err());
        assert!(!paths[0].exists(), "incomplete first shard leaked after failure");
        fs::remove_dir(&paths[1]).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod empty_partition_bucket_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn rejects_zero_buckets_without_panicking() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Integer(7)]]).unwrap();
        assert!(bucket(&batch, 0, &[0], 0).is_err());
        assert_eq!(bucket(&batch, 0, &[0], 1).unwrap(), 0);
    }
}

#[cfg(test)]
mod bounded_open_partition_writer_tests {
    use super::*;
    use crate::{hash_partition, scalar::Value, schema::{DataType, Field}, scale::spill::SpillReader};
    use std::{fs, io::BufReader};

    #[test]
    fn evicted_shards_append_without_duplicate_headers_or_lost_rows() {
        let dir = std::env::temp_dir().join(format!(
            "native-shard-lru-{}-{}", std::process::id(),
            PARTITION_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..2048)
            .map(|key| vec![Value::Integer(key)]).collect()).unwrap();
        let mut writer = PartitionWriter::new(&dir, schema, &["key"], 128, 37).unwrap();
        writer.write_batch(&batch, &QueryControl::default()).unwrap();
        assert!(writer.open_order.len() <= MAX_OPEN_SHARDS);
        assert!(writer.initialized.iter().filter(|&&present| present).count() > MAX_OPEN_SHARDS);
        writer.write_batch(&batch, &QueryControl::default()).unwrap();
        assert!(writer.open_order.len() <= MAX_OPEN_SHARDS);
        let files = writer.finish().unwrap();
        let expected = hash_partition::partition(&batch, &["key"], 128).unwrap();
        for (path, partition) in files.paths().iter().zip(expected.iter()) {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 37).unwrap();
            let mut actual = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() {
                actual.extend(batch.rows().unwrap());
            }
            let expected_rows = partition.rows().unwrap();
            assert_eq!(actual, expected_rows.iter().chain(expected_rows.iter())
                .cloned().collect::<Vec<_>>());
        }
        drop(files);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(&dir).unwrap();
    }
}

#[cfg(test)]
mod prebound_spill_keys_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn borrowed_column_hashes_match_checked_row_keys_at_many_fanouts() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("tag", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema, (0..2048_i64).map(|i| vec![
            if i % 17 == 0 { Value::Null } else { Value::Integer(i % 127) },
            if i % 31 == 0 { Value::Null } else { Value::Text(format!("tag-{}", i % 37)) },
        ]).collect()).unwrap();
        let columns = [batch.column(0).unwrap(), batch.column(1).unwrap()];
        for count in [2, 4, 31, 128, 1024, 4096] {
            for row in 0..batch.num_rows() {
                assert_eq!(bucket_bound(&columns, row, count),
                    bucket(&batch, row, &[0, 1], count).unwrap());
            }
        }
    }
}

#[cfg(test)]
mod reusable_sparse_partition_buffers_tests {
    use super::*;
    use std::{fs, io::BufReader, sync::atomic::{AtomicU64, Ordering as TestOrdering}};
    use crate::{schema::{DataType, Field}, scale::spill::SpillReader};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn sparse_shards_reuse_row_buffers_across_batches_without_leaking_rows() {
        let dir = std::env::temp_dir().join(format!("sparse-shards-{}-{}",
            std::process::id(), ID.fetch_add(1, TestOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let inputs = [
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(1)],
            ]).unwrap(),
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Integer(2)], vec![Value::Null], vec![Value::Integer(3)],
            ]).unwrap(),
        ];
        let mut writer = PartitionWriter::new(&dir, schema, &["key"], 16, 2).unwrap();
        for batch in &inputs {
            writer.write_batch(batch, &QueryControl::default()).unwrap();
        }
        let files = writer.finish().unwrap();
        let expected = crate::hash_partition::partition_batches(&inputs, &["key"], 16).unwrap();
        for (index, path) in files.paths().iter().enumerate() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 4).unwrap();
            let mut rows = Vec::new();
            while let Some(batch) = reader.next_batch().unwrap() {
                rows.extend(batch.rows().unwrap());
            }
            assert_eq!(rows, expected[index].rows().unwrap(), "shard {index}");
        }
        drop(files);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod repeated_shard_lru_tests {
    use super::*;

    #[test]
    fn repeated_hot_shard_writes_keep_unique_lru_entries() {
        let schema = Schema::new(vec![]).unwrap();
        let directory = std::env::temp_dir();
        let path = directory.join(format!("rustspark-hot-lru-{}-{}.rspill",
            std::process::id(), PARTITION_ID.fetch_add(1, Ordering::Relaxed)));
        let mut writers = vec![None];
        let mut order = WriterLru::new(1);
        let mut initialized = vec![false];
        let paths = [path.clone()];
        for _ in 0..128 {
            ensure_open(&mut writers, &mut order, &mut initialized,
                &paths, &schema, 0).unwrap();
            assert_eq!(order.iter().copied().collect::<Vec<_>>(), vec![0]);
        }
        writers[0].take().unwrap().finish().unwrap();
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod partition_wire_budget_regressions {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::io::Cursor;
    use super::super::spill::SpillReader;

    #[test]
    fn wire_budget_splits_borrowed_ranges_and_indexed_shards() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..4)
            .map(|n| vec![Value::Text(format!("{n}{}", "x".repeat(17)))])
            .collect()).unwrap();
        for selected in [vec![0usize, 1, 2, 3], vec![3usize, 1, 1, 0]] {
            let mut writer = SpillWriter::new(Vec::new(), schema.clone()).unwrap();
            let mut windows = Vec::new();
            write_bounded_windows(&batch, selected.len(), |i| selected[i],
                40, |start, end| {
                    windows.push(end - start);
                    writer.write_batch_indices(&batch, &selected[start..end])
                }).unwrap();
            assert_eq!(windows, vec![1, 1, 1, 1]);
            let mut reader = SpillReader::new(Cursor::new(writer.finish().unwrap()), 4).unwrap();
            let mut actual = Vec::new();
            while let Some(next) = reader.next_batch().unwrap() {
                actual.extend(next.rows().unwrap());
            }
            assert_eq!(actual, selected.iter().map(|&i| batch.row(i).unwrap()).collect::<Vec<_>>());
        }
    }

    #[test]
    fn one_row_larger_than_the_wire_budget_is_rejected_without_writing() {
        let schema = Schema::new(vec![Field::new("t", DataType::Text, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Text("a".repeat(64))]]).unwrap();
        let mut emitted = false;
        assert!(write_bounded_windows(&batch, 1, |i| i, 40,
            |_, _| { emitted = true; Ok(()) }).is_err());
        assert!(!emitted);
    }
}

#[cfg(test)]
mod indexed_shard_lru_regressions {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn evictions_preserve_recency_and_one_entry_per_shard() {
        let mut lru = WriterLru::new(128);
        for index in 0..64 { lru.insert(index); }
        for index in [0, 2, 1, 0, 63] { lru.touch(index); }
        assert_eq!(lru.len(), 64);
        assert_eq!(lru.pop_oldest(), Some(3));
        assert_eq!(lru.pop_oldest(), Some(4));
        lru.insert(80);
        lru.insert(81);
        let entries = lru.iter().copied().collect::<Vec<_>>();
        assert_eq!(entries.len(), 64);
        assert_eq!(&entries[entries.len() - 2..], &[80, 81]);
        assert_eq!(entries.iter().copied().collect::<HashSet<_>>().len(), 64);
        assert_eq!(entries.iter().position(|&index| index == 0), Some(60));
        lru.touch(80);
        assert_eq!(lru.iter().copied().last(), Some(80));
    }

    #[test]
    fn repeated_single_shard_hits_leave_links_unchanged() {
        let mut lru = WriterLru::new(1);
        lru.insert(0);
        for _ in 0..1024 {
            lru.touch(0);
            assert_eq!(lru.len(), 1);
            assert_eq!(lru.oldest, Some(0));
            assert_eq!(lru.newest, Some(0));
        }
        assert_eq!(lru.pop_oldest(), Some(0));
        assert_eq!(lru.pop_oldest(), None);
    }
}

#[cfg(test)]
mod consecutive_shuffle_key_run_regressions {
    use super::*;
    use super::super::spill::SpillReader;
    use crate::schema::{DataType, Field};
    use std::{fs, io::BufReader};
    use std::sync::atomic::{AtomicU64, Ordering as TestOrdering};

    static RUN: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn clustered_composite_keys_keep_identical_spill_partitions_across_windows() {
        let directory = std::env::temp_dir().join(format!("shuffle-runs-{}-{}",
            std::process::id(), RUN.fetch_add(1, TestOrdering::Relaxed)));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("number", DataType::Float, true),
            Field::new("label", DataType::Text, true),
            Field::new("rowid", DataType::Integer, false),
        ]).unwrap();
        let long = "é🙂".repeat(4096);
        let mut rows = Vec::new();
        for (number, label) in [
            (Value::Float(-0.0), Value::Text(long.clone())),
            (Value::Float(0.0), Value::Text(long.clone())),
            (Value::Float(f64::NAN), Value::Null),
            (Value::Null, Value::Text("null".into())),
            (Value::Float(9.0), Value::Text("nine".into())),
        ] {
            for _ in 0..27 {
                rows.push(vec![number.clone(), label.clone(),
                    Value::Integer(rows.len() as i64)]);
            }
        }
        let batch = Batch::from_rows(schema.clone(), rows).unwrap();
        for partitions in [2, 64] {
            let expected = crate::hash_partition::partition(&batch,
                &["number", "label"], partitions).unwrap();
            let mut writer = PartitionWriter::new(
                &directory, schema.clone(), &["number", "label"], partitions, 11,
            ).unwrap();
            writer.write_batch(&batch, &QueryControl::default()).unwrap();
            let files = writer.finish().unwrap();
            for (index, path) in files.paths().iter().enumerate() {
                let mut reader = SpillReader::new(
                    BufReader::new(File::open(path).unwrap()), 11,
                ).unwrap();
                let mut chunks = Vec::new();
                while let Some(chunk) = reader.next_batch().unwrap() {
                    chunks.push(chunk);
                }
                let output = crate::batch_utils::concat(
                    &schema, chunks, batch.num_rows(),
                ).unwrap();
                assert_eq!(output, expected[index],
                    "spill partition {index} of {partitions}");
            }
            drop(files);
        }
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod partition_writer_transactional_failure_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn invalid_later_shard_window_cannot_be_finalized_as_partial_result() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-partition-poison-{}-{}", std::process::id(),
            PARTITION_ID.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, false),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("valid".into())],
            vec![Value::Text("x".repeat(8 * 1024 * 1024 + 1))],
        ]).unwrap();
        let mut writer = PartitionWriter::new(&directory, schema, &["key"], 1, 1).unwrap();
        assert!(writer.write_batch(&input, &QueryControl::default()).is_err());
        assert!(writer.write_batch(&input, &QueryControl::default()).is_err());
        assert!(writer.finish().is_err());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(directory).unwrap();
    }
}
