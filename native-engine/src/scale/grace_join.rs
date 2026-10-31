//! Disk-partitioned equijoins. The build side is bounded per partition.
use std::{
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
const MAX_BUILD_BYTES: usize = 64 * 1024 * 1024;

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
    load_build_with_budget(path, schema, max_rows, io_rows, MAX_BUILD_BYTES, control)
}

fn load_build_with_budget(
    path: &Path, schema: &Schema, max_rows: usize, io_rows: usize,
    max_bytes: usize, control: &QueryControl,
) -> Result<Batch> {
    let mut reader = SpillReader::new(BufReader::new(File::open(path)?), io_rows)?;
    if reader.schema() != schema {
        return Err(Error::Schema("join build schema mismatch".into()));
    }
    let mut columns: Vec<Vec<Value>> = (0..schema.len()).map(|_| Vec::new()).collect();
    let mut rows = 0usize;
    let mut retained_bytes = 0usize;
    while let Some(batch) = reader.next_batch()? {
        control.check()?;
        let next = rows.checked_add(batch.num_rows())
            .filter(|total| *total <= max_rows)
            .ok_or_else(|| Error::Execution("join build partition exceeds row budget".into()))?;
        let source_columns = (0..schema.len())
            .map(|column| batch.column(column)).collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            if row % 1024 == 0 { control.check()?; }
            let bytes = super::row_footprint::footprint(
                source_columns.iter().map(|column| &column[row]));
            retained_bytes = retained_bytes.checked_add(bytes)
                .filter(|total| *total <= max_bytes)
                .ok_or_else(|| Error::Execution(
                    "join build partition exceeds byte budget".into()))?;
        }
        for (source, output) in source_columns.iter().zip(columns.iter_mut()) {
            output.extend_from_slice(source);
        }
        rows = next;
    }
    Batch::new_with_row_count(schema.clone(), columns, rows)
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
        // With no build rows, these join kinds cannot emit a match. The
        // probe spill may be huge, so bypass its row decoding and hash build.
        // LEFT, FULL, and ANTI must still visit the probe side.
        if build.num_rows() == 0 &&
            matches!(kind, JoinKind::Inner | JoinKind::Right | JoinKind::Semi) {
            writer.finish()?;
            continue;
        }
        // A RIGHT or FULL join must emit build-only rows once per partition,
        // not once for every probe batch.
        let outer_build = matches!(kind, JoinKind::Right | JoinKind::Full);
        let mut matched_right = outer_build.then(|| vec![false; build.num_rows()]);
        let probe_kind = match kind {
            JoinKind::Right => JoinKind::Inner,
            JoinKind::Full => JoinKind::Left,
            other => other,
        };
        // Build once per on-disk shard instead of once for every 256 probe rows.
        let prepared = join::PreparedHashJoin::new(
            left_schema.clone(), &build, left_key, right_key, probe_kind,
        )?;
        while let Some(batch) = reader.next_batch()? {
            control.check()?;
            let mut start = 0;
            while start < batch.num_rows() {
                let end = start.saturating_add(io_batch_rows.min(256)).min(batch.num_rows());
                let joined = prepared.probe_range_with_matches(
                    &batch, start, end, matched_right.as_deref_mut(),
                )?;
                start = end;
                // Align only metadata once, then encode row windows from
                // the same column buffers without allocating temporary chunks.
                let joined = if outer_build {
                    joined.into_widened_schema(result_schema.clone())?
                } else { joined };
                let mut out_start = 0;
                while out_start < joined.num_rows() {
                    let out_end = out_start.saturating_add(io_batch_rows).min(joined.num_rows());
                    writer.write_batch_range(&joined, out_start, out_end)?;
                    out_start = out_end;
                }
            }
        }
        if outer_build {
            let mut columns: Vec<Vec<Value>> =
                (0..result_schema.len()).map(|_| Vec::new()).collect();
            let build_columns = (0..right_schema.len())
                .map(|index| build.column(index)).collect::<Result<Vec<_>>>()?;
            let mut rows = 0usize;
            for right_row in 0..build.num_rows() {
                control.check()?;
                let matched = matched_right.as_ref()
                    .and_then(|bits| bits.get(right_row))
                    .ok_or_else(|| Error::Execution("missing outer-join match state".into()))?;
                if !*matched {
                    for output in &mut columns[..left_schema.len()] {
                        output.push(Value::Null);
                    }
                    for (input, output) in build_columns.iter()
                        .zip(columns[left_schema.len()..].iter_mut()) {
                        output.push(input[right_row].clone());
                    }
                    rows += 1;
                    if rows >= io_batch_rows {
                        let batch = Batch::new_with_row_count(
                            result_schema.clone(), std::mem::take(&mut columns), rows,
                        )?;
                        writer.write_batch(&batch)?;
                        columns = (0..result_schema.len()).map(|_| Vec::new()).collect();
                        rows = 0;
                    }
                }
            }
            if rows != 0 {
                writer.write_batch(&Batch::new_with_row_count(result_schema.clone(), columns, rows)?)?;
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

#[cfg(test)]
mod borrowed_spill_outer_keys_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::fs;

    #[test]
    fn repeated_long_text_keys_track_outer_matches_across_probe_batches() {
        let directory = std::env::temp_dir().join(format!(
            "grace-borrow-perf-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Text, true)]).unwrap();
        let common = Value::Text("shared-key-".repeat(128));
        let left = vec![
            Batch::from_rows(schema.clone(), vec![vec![common.clone()], vec![Value::Null]]).unwrap(),
            Batch::from_rows(schema.clone(), vec![
                vec![common.clone()], vec![Value::Text("only-left".into())]
            ]).unwrap(),
        ];
        let right = Batch::from_rows(schema.clone(), vec![
            vec![common.clone()], vec![common.clone()],
            vec![Value::Text("only-right".into())], vec![Value::Null],
        ]).unwrap();
        let output = grace_join(left.into_iter().map(Ok), [Ok(right)],
            schema.clone(), schema, "k", "k", JoinKind::Full,
            &directory, 1, 12, 1, &QueryControl::default()).unwrap();
        let mut rows = Vec::new();
        for path in output.paths() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 1).unwrap();
            while let Some(batch) = reader.next_batch().unwrap() {
                rows.extend(batch.rows().unwrap());
            }
        }
        assert_eq!(rows.iter().filter(|row| row[0] == common && row[1] == common).count(), 4);
        assert_eq!(rows.iter().filter(|row|
            row[0] == Value::Null && row[1] == Value::Text("only-right".into())).count(), 1);
        assert_eq!(rows.iter().filter(|row|
            row[0] == Value::Text("only-left".into()) && row[1] == Value::Null).count(), 1);
        assert_eq!(rows.iter().filter(|row| row == &&vec![Value::Null, Value::Null]).count(), 2);
        assert_eq!(rows.len(), 8);
        drop(output);
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod columnar_build_load_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn reads_nullable_text_builds_and_enforces_budget() {
        let dir = std::env::temp_dir().join(format!("native-column-build-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("build.rspill");
        let schema = Schema::new(vec![Field::new("k", DataType::Text, true)]).unwrap();
        let a = Batch::from_rows(schema.clone(), vec![vec![Value::Text("hello".into())], vec![Value::Null]]).unwrap();
        let b = Batch::from_rows(schema.clone(), vec![vec![Value::Text("world".into())]]).unwrap();
        let file = File::create(&path).unwrap();
        let mut writer = SpillWriter::new(BufWriter::new(file), schema.clone()).unwrap();
        writer.write_batch(&a).unwrap();
        writer.write_batch(&b).unwrap();
        writer.finish().unwrap();
        let loaded = load_build(&path, &schema, 3, 3, &QueryControl::default()).unwrap();
        assert_eq!(loaded.rows().unwrap(), vec![vec![Value::Text("hello".into())], vec![Value::Null], vec![Value::Text("world".into())]]);
        assert!(load_build(&path, &schema, 2, 3, &QueryControl::default()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod columnar_outer_unmatched_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn emits_wide_unmatched_right_rows_across_output_windows() {
        let dir = std::env::temp_dir().join(format!("native-column-outer-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true),
            Field::new("payload", DataType::Text, false)]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![vec![Value::Integer(1), Value::Text("probe".into())]]).unwrap();
        let right = Batch::from_rows(schema.clone(), (0..13).map(|n| vec![
            Value::Integer(n), Value::Text(format!("value-{n}-{}", "x".repeat(100)))
        ]).collect()).unwrap();
        let output = grace_join([Ok(left)], [Ok(right)], schema.clone(), schema, "k", "k", JoinKind::Right,
            &dir, 1, 20, 2, &QueryControl::default()).unwrap();
        let mut reader = SpillReader::new(BufReader::new(File::open(&output.paths()[0]).unwrap()), 2).unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() { rows.extend(batch.rows().unwrap()); }
        assert_eq!(rows.len(), 13);
        assert_eq!(rows.iter().filter(|row| row[0] == Value::Integer(1)).count(), 1);
        assert_eq!(rows.iter().filter(|row| row[0] == Value::Null).count(), 12);
        drop(output);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod empty_build_grace_join_short_circuit_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    use std::fs;

    #[test]
    fn empty_build_shards_return_typed_empty_outputs_for_inner_right_semi() {
        let dir = std::env::temp_dir().join(format!("native-empty-build-{}-{}",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, true)]).unwrap();
        let left = Batch::from_rows(schema.clone(), (0..60)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let right = Batch::empty(schema.clone());
        for kind in [JoinKind::Inner, JoinKind::Right, JoinKind::Semi] {
            let files = grace_join([Ok(left.clone())], [Ok(right.clone())],
                schema.clone(), schema.clone(), "k", "k", kind,
                &dir, 4, 100, 7, &QueryControl::default()).unwrap();
            let mut rows = 0usize;
            for path in files.paths() {
                let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 7).unwrap();
                while let Some(batch) = reader.next_batch().unwrap() {
                    rows += batch.num_rows();
                }
            }
            assert_eq!(rows, 0);
            drop(files);
        }
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod grace_join_build_byte_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    use std::{fs, io::BufWriter};

    #[test]
    fn rejects_wide_builds_even_below_row_count_limit() {
        let dir = std::env::temp_dir().join(format!(
            "native-join-build-budget-{}-{}", std::process::id(),
            JOIN_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("build.rspill");
        let schema = Schema::new(vec![Field::new("key", DataType::Text, false)]).unwrap();
        let expected = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("large".repeat(400))],
            vec![Value::Text("small".into())],
        ]).unwrap();
        let mut writer = SpillWriter::new(BufWriter::new(File::create(&path).unwrap()),
            schema.clone()).unwrap();
        writer.write_batch(&expected).unwrap();
        writer.finish().unwrap();
        let control = QueryControl::default();
        let error = load_build_with_budget(&path, &schema, 10, 2, 1024, &control)
            .unwrap_err();
        assert!(error.to_string().contains("byte budget"));
        assert_eq!(load_build_with_budget(&path, &schema, 10, 2, 8192,
            &control).unwrap(), expected);
        assert!(load_build_with_budget(&path, &schema, 1, 2, 8192,
            &control).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
