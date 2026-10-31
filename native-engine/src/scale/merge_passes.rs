//! Multi-pass external merge to bound open file descriptors.
use std::{
    fs::OpenOptions,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{schema::Schema, sort::SortKey, Error, Result};
use super::{
    cancel::QueryControl, merge_runs::RunMerger,
    sorted_runs::RunSet, spill::SpillWriter,
};

static PASS_ID: AtomicU64 = AtomicU64::new(1);

pub struct MultiPassRuns {
    _source: RunSet,
    scratch: Vec<PathBuf>,
    current: Vec<PathBuf>,
}

impl MultiPassRuns {
    pub fn files(&self) -> &[PathBuf] { &self.current }
}

impl Drop for MultiPassRuns {
    fn drop(&mut self) {
        for path in &self.scratch { let _ = std::fs::remove_file(path); }
    }
}

pub fn merge_to_fan_in(
    source: RunSet, directory: &Path, schema: Schema, keys: &[SortKey],
    fan_in: usize, io_batch_rows: usize, control: &QueryControl
) -> Result<MultiPassRuns> {
    if !(2..=64).contains(&fan_in) {
        return Err(Error::Execution("external merge fan-in must be 2..=64".into()));
    }
    if !(1..=65_536).contains(&io_batch_rows) {
        return Err(Error::Execution("external merge I/O batch size must be in 1..=65536".into()));
    }
    if source.schema() != &schema {
        return Err(Error::Schema("multi-pass sort schema mismatch".into()));
    }
    let mut result = MultiPassRuns {
        current: source.files().to_vec(), _source: source, scratch: Vec::new()
    };
    while result.current.len() > fan_in {
        control.check()?;
        // Each fan-in group emits exactly one path in the next pass.
        let mut next_pass = Vec::with_capacity(result.current.len().div_ceil(fan_in));
        for group in result.current.chunks(fan_in) {
            control.check()?;
            if group.len() == 1 {
                next_pass.push(group[0].clone());
                continue;
            }
            let id = PASS_ID.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!("rustspark-merge-{}-{id}.rspill", std::process::id()));
            let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
            result.scratch.push(path.clone());
            let merger = RunMerger::new(
                group, schema.clone(), keys, io_batch_rows, io_batch_rows, control.clone()
            )?;
            let mut writer = SpillWriter::new(BufWriter::new(file), schema.clone())?;
            for batch in merger { control.check()?; writer.write_batch(&batch?)?; }
            writer.finish()?;
            next_pass.push(path);
        }
        result.current = next_pass;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        batch::Batch, expr::Expr, scalar::Value,
        schema::{DataType, Field}, scale::sorted_runs::write_sorted_runs
    };
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn multi_pass_sort_handles_many_spill_runs() {
        let dir = std::env::temp_dir().join(format!("native-passes-{}-{}",
            std::process::id(), TEST_ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..101).rev()
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("value".into()))];
        let source = write_sorted_runs([Ok(batch)], schema.clone(), &keys, &dir, 3,
            &QueryControl::default()).unwrap();
        let merged = merge_to_fan_in(source, &dir, schema.clone(), &keys, 4, 3,
            &QueryControl::default()).unwrap();
        assert!(merged.files().len() <= 4);
        let final_merge = RunMerger::new(merged.files(), schema, &keys, 3, 8,
            QueryControl::default()).unwrap();
        let all = final_merge.collect::<Result<Vec<_>>>().unwrap();
        let rows = all.iter().flat_map(|batch| batch.rows().unwrap())
            .map(|row| row[0].clone()).collect::<Vec<_>>();
        assert_eq!(rows, (0..101).map(Value::Integer).collect::<Vec<_>>());
        drop(merged);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod merge_batch_size_diagnostics_tests {
    use super::*;
    use crate::batch::Batch;
    #[test]
    fn separates_invalid_fan_in_from_invalid_io_batch_size() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "key", crate::schema::DataType::Integer, false)]).unwrap();
        let keys = [SortKey::ascending(crate::expr::Expr::Column("key".into()))];
        let empty = || write_source(schema.clone());
        let err = merge_to_fan_in(empty(), Path::new("."), schema.clone(), &keys,
            4, 0, &QueryControl::default()).err().unwrap();
        assert!(err.to_string().contains("batch size"));
        let err = merge_to_fan_in(empty(), Path::new("."), schema, &keys,
            1, 4, &QueryControl::default()).err().unwrap();
        assert!(err.to_string().contains("fan-in"));
    }
    fn write_source(schema: Schema) -> RunSet {
        super::super::sorted_runs::write_sorted_runs(
            std::iter::empty::<Result<Batch>>(), schema,
            &[SortKey::ascending(crate::expr::Expr::Column("key".into()))],
            Path::new("."), 1, &QueryControl::default()
        ).unwrap()
    }
}

#[cfg(test)]
mod merge_pass_large_batch_rejection_tests {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field}};
    #[test]
    fn rejects_invalid_batch_sizes_even_with_no_merge_work() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false)]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let source = super::super::sorted_runs::write_sorted_runs(
            std::iter::empty::<Result<crate::batch::Batch>>(), schema.clone(),
            &keys, Path::new("."), 1, &QueryControl::default()).unwrap();
        assert!(merge_to_fan_in(source, Path::new("."), schema, &keys,
            2, 65_537, &QueryControl::default()).is_err());
    }
}
