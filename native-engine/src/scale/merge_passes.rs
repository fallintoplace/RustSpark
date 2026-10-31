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
        // Only the final short group can contain one input run; every
        // other old run was consumed by a newly produced output. Preserve
        // that single carried path without hashing all next-pass filenames.
        let has_singleton = result.current.len() % fan_in == 1;
        for (index, path) in result.current.iter().enumerate() {
            if has_singleton && index + 1 == result.current.len() {
                continue;
            }
            std::fs::remove_file(path)?;
        }
        let carried = has_singleton.then(|| result.current.last().expect("nonempty pass"));
        result.scratch.retain(|path| carried.is_some_and(|keep| keep == path));
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

#[cfg(test)]
mod retired_merge_scratch_tests {
    use super::*;
    use crate::{
        batch::Batch, expr::Expr, scalar::Value,
        schema::{DataType, Field}, scale::sorted_runs::write_sorted_runs,
    };
    use std::fs;

    #[test]
    fn deletes_previous_passes_but_keeps_current_runs_readable() {
        let dir = std::env::temp_dir().join(format!(
            "native-merge-gc-{}-{}", std::process::id(),
            PASS_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let source = Batch::from_rows(schema.clone(), (0..128).rev()
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let runs = write_sorted_runs([Ok(source)], schema.clone(), &keys, &dir, 4,
            &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 32);
        let merged = merge_to_fan_in(runs, &dir, schema.clone(), &keys, 2, 4,
            &QueryControl::default()).unwrap();
        let active_scratch = fs::read_dir(&dir).unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap().to_string_lossy()
                .starts_with("rustspark-merge-"))
            .collect::<Vec<_>>();
        assert_eq!(active_scratch.len(), merged.files().len());
        assert_eq!(active_scratch.len(), 2);
        assert!(active_scratch.iter().all(|path| merged.files().contains(path)));
        let rows = RunMerger::new(merged.files(), schema, &keys, 4, 7,
            QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.iter().flat_map(|b| b.rows().unwrap())
            .map(|row| row[0].clone()).collect::<Vec<_>>(),
            (0..128).map(Value::Integer).collect::<Vec<_>>());
        drop(merged);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod reclaim_source_runs_after_merge_tests {
    use super::*;
    use crate::{
        batch::Batch, expr::Expr, scalar::Value,
        schema::{DataType, Field}, scale::sorted_runs::write_sorted_runs,
    };
    use std::fs;

    #[test]
    fn releases_original_and_intermediate_files_before_final_merge() {
        let dir = std::env::temp_dir().join(format!(
            "native-merge-all-gc-{}-{}", std::process::id(),
            PASS_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let batch = Batch::from_rows(schema.clone(),
            (0..21).rev().map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let source = write_sorted_runs([Ok(batch)], schema.clone(), &keys, &dir, 2,
            &QueryControl::default()).unwrap();
        let originals = source.files().to_vec();
        assert_eq!(originals.len(), 11);
        let merged = merge_to_fan_in(source, &dir, schema.clone(), &keys, 2, 2,
            &QueryControl::default()).unwrap();
        assert_eq!(merged.files().len(), 2);
        assert!(originals.iter().all(|path| !path.exists()));
        let live = fs::read_dir(&dir).unwrap().map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(live.len(), 2);
        assert!(live.iter().all(|path| merged.files().contains(path)));
        let output = RunMerger::new(merged.files(), schema, &keys, 2, 3,
            QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().flat_map(|batch| batch.rows().unwrap())
            .map(|row| row[0].clone()).collect::<Vec<_>>(),
            (0..21).map(Value::Integer).collect::<Vec<_>>());
        drop(merged);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod merge_singleton_cleanup_regressions {
    use super::*;
    use crate::{batch::Batch, expr::Expr, scalar::Value, schema::{DataType, Field},
        scale::sorted_runs::write_sorted_runs};

    #[test]
    fn lone_run_survives_successive_merge_passes_without_leaking_scratch() {
        let directory = std::env::temp_dir().join(format!(
            "rustspark-merge-singletons-{}-{}", std::process::id(),
            PASS_ID.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(), (0..10).rev()
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("k".into()))];
        let source = write_sorted_runs([Ok(input)], schema.clone(), &keys,
            &directory, 1, &QueryControl::default()).unwrap();
        assert_eq!(source.files().len(), 10);
        let merged = merge_to_fan_in(source, &directory, schema.clone(), &keys,
            3, 2, &QueryControl::default()).unwrap();
        assert_eq!(merged.files().len(), 2);
        let all = RunMerger::new(merged.files(), schema, &keys,
            2, 4, QueryControl::default()).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        let values = all.iter().flat_map(|batch| batch.rows().unwrap())
            .map(|row| row[0].clone()).collect::<Vec<_>>();
        assert_eq!(values, (0..10).map(Value::Integer).collect::<Vec<_>>());
        drop(merged);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
        std::fs::remove_dir(directory).unwrap();
    }
}
