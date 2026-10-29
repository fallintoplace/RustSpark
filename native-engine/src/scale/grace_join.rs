//! Disk-partitioned equijoins. The build side is bounded per partition.
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{
    batch::Batch, join::{self, JoinKind}, scalar::Value,
    schema::Schema, Error, Result,
};
use super::{
    cancel::QueryControl, chunks::BatchChunks,
    partition_spill::PartitionWriter, spill::{SpillReader, SpillWriter},
};

static JOIN_ID: AtomicU64 = AtomicU64::new(1);

pub struct JoinFiles {
    paths: Vec<PathBuf>,
    schema: Schema,
}

impl JoinFiles {
    pub fn paths(&self) -> &[PathBuf] { &self.paths }
    pub fn schema(&self) -> &Schema { &self.schema }
}

impl Drop for JoinFiles {
    fn drop(&mut self) {
        for path in &self.paths { let _ = std::fs::remove_file(path); }
    }
}

fn load_build(
    path: &Path, schema: &Schema, max_rows: usize, io_rows: usize,
    control: &QueryControl,
) -> Result<Batch> {
    let mut reader = SpillReader::new(BufReader::new(File::open(path)?), io_rows)?;
    if reader.schema() != schema {
        return Err(Error::Schema("join build schema mismatch".into()));
    }
    let mut rows = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        control.check()?;
        if rows.len().checked_add(batch.num_rows()).is_none_or(|n| n > max_rows) {
            return Err(Error::Execution("join build partition exceeds row budget".into()));
        }
        rows.extend(batch.rows()?);
    }
    Batch::from_rows(schema.clone(), rows)
}

#[allow(clippy::too_many_arguments)]
pub fn grace_join<L, R>(
    left: L, right: R, left_schema: Schema, right_schema: Schema,
    left_key: &str, right_key: &str, kind: JoinKind, directory: &Path,
    partitions: usize, max_build_rows: usize, io_batch_rows: usize,
    control: &QueryControl,
) -> Result<JoinFiles>
where L: IntoIterator<Item = Result<Batch>>,
      R: IntoIterator<Item = Result<Batch>>
{
    if max_build_rows == 0 || io_batch_rows == 0 {
        return Err(Error::Execution("join requires positive memory and IO limits".into()));
    }
    let left_index = left_schema.column(left_key)?;
    let right_index = right_schema.column(right_key)?;
    if left_schema.field(left_index)?.data_type != right_schema.field(right_index)?.data_type {
        return Err(Error::Type("join key types must match".into()));
    }

    let mut lhs = PartitionWriter::new(
        directory, left_schema.clone(), &[left_key], partitions, io_batch_rows)?;
    let mut rhs = PartitionWriter::new(
        directory, right_schema.clone(), &[right_key], partitions, io_batch_rows)?;
    for incoming in left {
        control.check()?;
        lhs.write_batch(&incoming?, control)?;
    }
    for incoming in right {
        control.check()?;
        rhs.write_batch(&incoming?, control)?;
    }
    let lhs_files = lhs.finish()?;
    let rhs_files = rhs.finish()?;
    let result_schema = join::hash_join(
        &Batch::empty(left_schema.clone()), &Batch::empty(right_schema.clone()),
        left_key, right_key, kind
    )?.schema().clone();
    let mut output = JoinFiles { paths: Vec::new(), schema: result_schema.clone() };

    for (left_path, right_path) in lhs_files.paths().iter().zip(rhs_files.paths()) {
        control.check()?;
        let build = load_build(
            right_path, &right_schema, max_build_rows, io_batch_rows, control)?;
        let mut reader = SpillReader::new(
            BufReader::new(File::open(left_path)?), io_batch_rows)?;
        if reader.schema() != &left_schema {
            return Err(Error::Schema("join probe schema mismatch".into()));
        }
        let id = JOIN_ID.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("rustspark-join-{}-{id}.rspill", std::process::id()));
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        output.paths.push(path);
        let mut writer = SpillWriter::new(BufWriter::new(file), result_schema.clone())?;
        // A RIGHT or FULL join must emit build-only rows once per partition,
        // not once for every probe batch.
        let outer_build = matches!(kind, JoinKind::Right | JoinKind::Full);
        let mut build_keys = HashSet::new();
        if outer_build {
            for key in build.column(right_index)? {
                if !key.is_null() { build_keys.insert(key.clone()); }
            }
        }
        let mut matched_keys = HashSet::new();
        let probe_kind = match kind {
            JoinKind::Right => JoinKind::Inner,
            JoinKind::Full => JoinKind::Left,
            other => other,
        };
        while let Some(batch) = reader.next_batch()? {
            control.check()?;
            for piece in BatchChunks::new(&batch, io_batch_rows.min(256))? {
                let piece = piece?;
                if outer_build {
                    for key in piece.column(left_index)? {
                        if !key.is_null() && build_keys.contains(key) {
                            matched_keys.insert(key.clone());
                        }
                    }
                }
                let joined = join::hash_join(&piece, &build, left_key, right_key, probe_kind)?;
                for output_chunk in BatchChunks::new(&joined, io_batch_rows)? {
                    let output_chunk = output_chunk?;
                    if output_chunk.num_rows() != 0 {
                        if outer_build {
                            // The FULL/RIGHT output schema marks padded fields nullable.
                            let aligned = Batch::from_rows(
                                result_schema.clone(), output_chunk.rows()?
                            )?;
                            writer.write_batch(&aligned)?;
                        } else {
                            writer.write_batch(&output_chunk)?;
                        }
                    }
                }
            }
        }
        if outer_build {
            let mut pending = Vec::with_capacity(io_batch_rows.min(65_536));
            for right_row in 0..build.num_rows() {
                control.check()?;
                let key = build.value(right_row, right_index)?;
                if key.is_null() || !matched_keys.contains(key) {
                    let mut row = vec![Value::Null; left_schema.len()];
                    row.extend(build.row(right_row)?);
                    pending.push(row);
                    if pending.len() >= io_batch_rows {
                        writer.write_batch(&Batch::from_rows(
                            result_schema.clone(), std::mem::take(&mut pending)
                        )?)?;
                    }
                }
            }
            if !pending.is_empty() {
                writer.write_batch(&Batch::from_rows(result_schema.clone(), pending)?)?;
            }
        }
        writer.finish()?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn left_join_preserves_null_and_unmatched_keys_after_partitioning() {
        let dir = std::env::temp_dir().join(format!("native-grace-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2)], vec![Value::Integer(1)],
            vec![Value::Null], vec![Value::Integer(3)]
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]).unwrap();
        let output = grace_join([Ok(left)], [Ok(right)],
            schema.clone(), schema, "id", "id", JoinKind::Left,
            &dir, 8, 20, 2, &QueryControl::default()).unwrap();
        let mut all = Vec::new();
        for path in output.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 2).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() { all.extend(batch.rows().unwrap()); }
        }
        assert_eq!(all.len(), 4);
        assert!(all.contains(&vec![Value::Integer(1), Value::Integer(1)]));
        assert!(all.contains(&vec![Value::Integer(2), Value::Integer(2)]));
        assert!(all.contains(&vec![Value::Integer(3), Value::Null]));
        assert!(all.contains(&vec![Value::Null, Value::Null]));
        drop(output);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod outer_join_partition_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};

    static OUTER_TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn right_and_full_outer_rows_are_emitted_once_across_probe_batches() {
        let dir = std::env::temp_dir().join(format!(
            "native-grace-outer-{}-{}", std::process::id(),
            OUTER_TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let left = vec![
            Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap(),
            Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1)]]).unwrap(),
            Batch::from_rows(schema.clone(), vec![vec![Value::Integer(3)]]).unwrap(),
        ];
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Integer(4)], vec![Value::Null],
        ]).unwrap();
        for kind in [JoinKind::Right, JoinKind::Full] {
            let output = grace_join(
                left.clone().into_iter().map(Ok),
                [Ok(right.clone())],
                schema.clone(), schema.clone(),
                "id", "id", kind, &dir, 1, 20, 1, &QueryControl::default()
            ).unwrap();
            let mut rows = Vec::new();
            for path in output.paths() {
                let mut reader = SpillReader::new(
                    BufReader::new(File::open(path).unwrap()), 1
                ).unwrap();
                while let Some(batch) = reader.next_batch().unwrap() {
                    rows.extend(batch.rows().unwrap());
                }
            }
            assert_eq!(rows.iter().filter(|row|
                row[0] == Value::Integer(1) && row[1] == Value::Integer(1)
            ).count(), 4);
            for unmatched in [Value::Integer(2), Value::Integer(4), Value::Null] {
                assert_eq!(rows.iter().filter(|row|
                    row[0] == Value::Null && row[1] == unmatched
                ).count(), 1);
            }
            assert_eq!(rows.iter().filter(|row|
                row[0] == Value::Integer(3) && row[1] == Value::Null
            ).count(), usize::from(kind == JoinKind::Full));
            assert_eq!(rows.len(), if kind == JoinKind::Full { 8 } else { 7 });
            drop(output);
        }
        fs::remove_dir(dir).unwrap();
    }
}
