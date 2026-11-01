//! Exact NULL-safe DISTINCT using disk partitions and bounded local hash sets.
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    fs::File,
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
const MAX_DISTINCT_STATE_BYTES: usize = 64 * 1024 * 1024;

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
        // Fingerprint borrowed columns before cloning into persistent state.
        // Equality inside each hash bucket still resolves collisions exactly.
        let mut seen: HashMap<u64, Vec<Vec<Value>>> = HashMap::new();
        let mut unique_rows = 0usize;
        let mut retained_bytes = 0usize;
        // Cache the last observed owned key by stable fingerprint bucket and
        // index. Unlike a borrow into the decoded batch, this survives its
        // deletion and avoids cloning wide TEXT keys between spill chunks.
        let mut recent_key: Option<(u64, usize)> = None;
        while let Some(batch) = reader.next_batch()? {
            control.check()?;
            // The spill decoder owns these columns. Move first-seen keys
            // into retained state instead of cloning large TEXT values.
            let row_count = batch.num_rows();
            let mut columns = batch.into_columns();
            let mut index = 0usize;
            while index < row_count {
                let row = index;
                if row % 1024 == 0 { control.check()?; }
                // Detect the whole adjacent duplicate run before moving any
                // owned cells out of its first row. Once moved, a borrowed
                // comparison against that first row would be incorrect.
                let mut next = row + 1;
                while next < row_count &&
                    columns.iter().all(|column| column[row] == column[next])
                {
                    if next % 1024 == 0 { control.check()?; }
                    next += 1;
                }
                index = next;
                // Equality must still be checked against all columns; hash
                // values alone cannot identify a row under collisions.
                if recent_key.is_some_and(|(hash, position)|
                    seen.get(&hash).and_then(|candidates| candidates.get(position))
                        .is_some_and(|key| key.iter().zip(&columns).all(|(value, column)|
                            value == &column[row]))
                ) {
                    continue;
                }
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                columns.len().hash(&mut hasher);
                for column in &columns { column[row].hash(&mut hasher); }
                let fingerprint = hasher.finish();
                if let Some(position) = seen.get(&fingerprint).and_then(|rows|
                    rows.iter().position(|candidate|
                        candidate.iter().zip(&columns).all(|(value, column)|
                            value == &column[row]))
                ) {
                    recent_key = Some((fingerprint, position));
                    continue;
                }
                if unique_rows >= max_unique_rows_per_partition {
                    return Err(Error::Execution("DISTINCT partition exceeds state budget".into()));
                }
                let footprint = super::row_footprint::footprint(
                    columns.iter().map(|column| &column[row])
                );
                let next_bytes = retained_bytes.checked_add(footprint)
                    .filter(|&bytes| bytes <= MAX_DISTINCT_STATE_BYTES)
                    .ok_or_else(|| Error::Execution("DISTINCT partition exceeds memory budget".into()))?;
                let row = columns.iter_mut()
                    .map(|column| std::mem::replace(&mut column[row], Value::Null))
                    .collect();
                let candidates = seen.entry(fingerprint).or_default();
                let position = candidates.len();
                candidates.push(row);
                recent_key = Some((fingerprint, position));
                unique_rows += 1;
                retained_bytes = next_bytes;
            }
        }
        // Untouched partitions have no output. Avoid writing and later
        // reopening a separate schema-only result file for each empty shard.
        if unique_rows == 0 { continue; }
        let id = DISTINCT_ID.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("rustspark-distinct-{}-{id}.rspill", std::process::id()));
        let file = super::private_files::create_private(&path)?;
        result.paths.push(path);
        let mut output = SpillWriter::new(BufWriter::new(file), schema.clone())?;
        // Every retained row was decoded from a validated spill batch.
        // Assemble columns directly instead of buffering row vectors and
        // transposing them once more during each output flush.
        let capacity = io_batch_rows.min(unique_rows).min(256);
        let mut columns: Vec<Vec<Value>> = (0..schema.len())
            .map(|_| Vec::with_capacity(capacity)).collect();
        let mut pending_rows = 0usize;
        for (index, row) in seen.into_values().flatten().enumerate() {
            if index % 1024 == 0 { control.check()?; }
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
            pending_rows += 1;
            if pending_rows == io_batch_rows {
                output.write_batch(&Batch::from_validated_columns(
                    schema.clone(), std::mem::take(&mut columns), pending_rows
                )?)?;
                columns = (0..schema.len())
                    .map(|_| Vec::with_capacity(capacity)).collect();
                pending_rows = 0;
            }
        }
        if pending_rows != 0 {
            output.write_batch(&Batch::from_validated_columns(
                schema.clone(), columns, pending_rows
            )?)?;
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

#[cfg(test)]
mod sparse_distinct_result_shards_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::fs;

    #[test]
    fn sparse_partitions_write_only_nonempty_distinct_shards() {
        let dir = std::env::temp_dir().join(format!("native-distinct-sparse-{}-{}",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(42)], vec![Value::Integer(42)],
        ]).unwrap();
        let output = distinct_to_disk([Ok(input)], schema.clone(),
            &dir, 64, 100, 2, &QueryControl::default()).unwrap();
        assert_eq!(output.paths().len(), 1);
        let mut reader = SpillReader::new(BufReader::new(
            File::open(&output.paths()[0]).unwrap()), 2).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Integer(42)]]);
        drop(output);

        let empty = distinct_to_disk(Vec::<Result<Batch>>::new(), schema,
            &dir, 8, 100, 2, &QueryControl::default()).unwrap();
        assert!(empty.paths().is_empty());
        drop(empty);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod borrowed_distinct_spill_membership_tests {
    use super::*;
    use std::{fs, io::BufReader, sync::atomic::{AtomicU64, Ordering as TestOrdering}};
    use crate::schema::{DataType, Field};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn repeated_wide_text_and_null_keys_stay_distinct_across_batches() {
        let dir = std::env::temp_dir().join(format!("borrowed-distinct-{}-{}",
            std::process::id(), ID.fetch_add(1, TestOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("id", DataType::Integer, true),
        ]).unwrap();
        let large = "é🙂".repeat(4096);
        let row = vec![Value::Text(large), Value::Integer(7)];
        let empty = vec![Value::Null, Value::Integer(1)];
        let first = Batch::from_rows(schema.clone(), vec![
            row.clone(), empty.clone(), row.clone(),
        ]).unwrap();
        let second = Batch::from_rows(schema.clone(), vec![
            row.clone(), empty.clone(), vec![Value::Text("other".into()), Value::Null],
        ]).unwrap();
        let files = distinct_to_disk([Ok(first), Ok(second)], schema.clone(),
            &dir, 8, 10, 3, &QueryControl::default()).unwrap();
        let mut found = Vec::new();
        for path in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 3).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                assert_eq!(batch.schema(), &schema);
                found.extend(batch.rows().unwrap());
            }
        }
        assert_eq!(found.len(), 3);
        assert!(found.contains(&row));
        assert!(found.contains(&empty));
        assert!(found.contains(&vec![Value::Text("other".into()), Value::Null]));
        drop(files);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod owned_distinct_spill_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn unique_large_text_and_repeated_null_keys_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-owned-distinct-{}-{}",
            std::process::id(),
            DISTINCT_ID.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Text, true)]).unwrap();
        let long = "🙂".repeat(20_000);
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text(long.clone())], vec![Value::Null],
            vec![Value::Text(long.clone())], vec![Value::Text("other".into())],
        ]).unwrap();
        let files = distinct_to_disk([Ok(input)], schema.clone(), &dir, 4, 4, 2,
            &QueryControl::default()).unwrap();
        let mut values = Vec::new();
        for path in files.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 16).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                values.extend(batch.rows().unwrap().into_iter().map(|row| row[0].clone()));
            }
        }
        assert_eq!(values.len(), 3);
        assert!(values.contains(&Value::Null));
        assert!(values.contains(&Value::Text(long)));
        assert!(values.contains(&Value::Text("other".into())));
        drop(files);
        std::fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod clustered_disk_distinct_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as TestOrdering};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn clustered_unicode_and_null_rows_survive_disk_exchange_without_duplicate_output() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-clustered-distinct-{}-{}",
            std::process::id(), ID.fetch_add(1, TestOrdering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("variant", DataType::Integer, true),
        ]).unwrap();
        let wide = "é🦀".repeat(1024);
        let mut rows = vec![vec![Value::Text(wide.clone()), Value::Integer(7)]; 256];
        rows.extend(vec![vec![Value::Null, Value::Null]; 256]);
        rows.extend(vec![vec![Value::Text(wide.clone()), Value::Integer(7)]; 128]);
        rows.push(vec![Value::Text("other".into()), Value::Integer(0)]);
        let input = Batch::from_rows(schema.clone(), rows).unwrap();
        let output = distinct_to_disk(
            [Ok(input)], schema, &directory, 8, 3, 512,
            &QueryControl::default(),
        ).unwrap();
        let mut selected = Vec::new();
        for path in output.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 512)
                .unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                selected.extend(batch.rows().unwrap());
            }
        }
        assert_eq!(selected.len(), 3);
        assert!(selected.contains(&vec![Value::Text(wide), Value::Integer(7)]));
        assert!(selected.contains(&vec![Value::Null, Value::Null]));
        assert!(selected.contains(&vec![Value::Text("other".into()), Value::Integer(0)]));
        drop(output);
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod cross_spill_disk_distinct_hot_key_regressions {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn wide_clustered_keys_remain_unique_across_many_tiny_spill_batches() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-distinct-cross-chunk-{}-{}", std::process::id(),
            TEST_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Text, true),
            Field::new("value", DataType::Integer, true),
        ]).unwrap();
        let first = vec![
            Value::Text("🦀".repeat(1_024)), Value::Integer(1),
        ];
        let null = vec![Value::Null, Value::Null];
        let second = vec![
            Value::Text("é".repeat(1_500)), Value::Integer(2),
        ];
        let mut batches = Vec::new();
        for index in 0..180 {
            let row = if index < 90 || index >= 150 {
                first.clone()
            } else if index < 120 {
                null.clone()
            } else {
                second.clone()
            };
            batches.push(Ok(Batch::from_rows(schema.clone(), vec![row]).unwrap()));
            if index % 13 == 0 {
                batches.push(Ok(Batch::empty(schema.clone())));
            }
        }
        let output = distinct_to_disk(
            batches, schema.clone(), &directory, 4, 8, 1,
            &QueryControl::default(),
        ).unwrap();
        let mut observed = std::collections::HashSet::new();
        for path in output.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 1).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                observed.extend(batch.rows().unwrap());
            }
        }
        let expected = [first, null, second].into_iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(observed, expected);
        drop(output);
        fs::remove_dir_all(directory).unwrap();
    }
}
