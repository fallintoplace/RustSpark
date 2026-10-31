//! Generate independently sorted spill runs with deterministic scratch cleanup.
use std::{
    fs::OpenOptions,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, schema::Schema, sort::{self, SortKey}, Error, Result};
use super::{cancel::QueryControl, spill::SpillWriter};
#[cfg(test)]
use std::fs::File;

static RUN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct RunSet {
    files: Vec<PathBuf>,
    schema: Schema,
}

impl RunSet {
    pub fn files(&self) -> &[PathBuf] { &self.files }

    pub fn schema(&self) -> &Schema { &self.schema }

    /// Ownership of these files stays with RunSet. Dropping it removes all runs.
    pub fn is_empty(&self) -> bool { self.files.is_empty() }
}

impl Drop for RunSet {
    fn drop(&mut self) {
        for path in &self.files { let _ = std::fs::remove_file(path); }
    }
}

fn validate_sort_keys(schema: &Schema, keys: &[SortKey]) -> Result<()> {
    if keys.is_empty() {
        return Err(Error::Execution("external sort needs at least one key".into()));
    }
    for key in keys {
        if !matches!(&key.expression, crate::expr::Expr::Column(_)) {
            return Err(Error::Unsupported("external merge requires column sort keys".into()));
        }
        key.expression.output_type(schema)?;
    }
    Ok(())
}

fn flush_pending_run(
    runs: &mut RunSet, pending: &mut Vec<Batch>, pending_rows: &mut usize,
    schema: &Schema, keys: &[SortKey], directory: &Path,
    max_rows_per_run: usize, control: &QueryControl,
) -> Result<()> {
    if *pending_rows == 0 { return Ok(()); }
    control.check()?;
    // Move complete incoming batches into a single run without re-cloning TEXT
    // just to stitch together a stream of small source batches.
    let input = crate::batch_utils::concat(
        schema, std::mem::take(pending), max_rows_per_run,
    )?;
    let sorted = sort::sort_owned(input, keys)?;
    *pending_rows = 0;
    let id = RUN_ID.fetch_add(1, Ordering::Relaxed);
    let filename = format!("rustspark-sort-{}-{id}.rspill", std::process::id());
    let path = directory.join(filename);
    let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
    runs.files.push(path);
    let mut writer = SpillWriter::new(BufWriter::new(file), schema.clone())?;
    writer.write_batch(&sorted)?;
    writer.finish()?;
    Ok(())
}

pub fn write_sorted_runs<I>(
    input: I, schema: Schema, keys: &[SortKey],
    directory: &Path, max_rows_per_run: usize, control: &QueryControl,
) -> Result<RunSet>
where I: IntoIterator<Item = Result<Batch>>
{
    if !(1..=65_536).contains(&max_rows_per_run) {
        return Err(Error::Execution("sort run size must be in 1..=65536".into()));
    }
    validate_sort_keys(&schema, keys)?;
    let mut runs = RunSet { files: Vec::new(), schema: schema.clone() };
    let mut pending = Vec::new();
    let mut pending_rows = 0usize;
    for incoming in input {
        control.check()?;
        let incoming = incoming?;
        if incoming.schema() != &schema {
            return Err(Error::Schema("external sort input schema changed".into()));
        }
        if incoming.num_rows() == 0 { continue; }
        if incoming.num_rows() <= max_rows_per_run - pending_rows {
            pending_rows += incoming.num_rows();
            pending.push(incoming);
            if pending_rows == max_rows_per_run {
                flush_pending_run(&mut runs, &mut pending, &mut pending_rows,
                    &schema, keys, directory, max_rows_per_run, control)?;
            }
        } else {
            // Split only batches that cross run boundaries. Entire small
            // batches are transferred, avoiding an extra per-row clone.
            let mut start = 0;
            while start < incoming.num_rows() {
                control.check()?;
                let take = (max_rows_per_run - pending_rows)
                    .min(incoming.num_rows() - start);
                let end = start + take;
                pending.push(incoming.slice_rows(start, end)?);
                pending_rows += take;
                start = end;
                if pending_rows == max_rows_per_run {
                    flush_pending_run(&mut runs, &mut pending, &mut pending_rows,
                        &schema, keys, directory, max_rows_per_run, control)?;
                }
            }
        }
    }
    flush_pending_run(&mut runs, &mut pending, &mut pending_rows,
        &schema, keys, directory, max_rows_per_run, control)?;
    Ok(runs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field},
        scale::spill::SpillReader};
    use std::io::BufReader;

    #[test]
    fn large_input_is_split_into_sorted_scratch_runs() {
        let dir = std::env::temp_dir().join(format!("native-sort-test-{}-{}",
            std::process::id(), RUN_ID.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..25).rev()
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let runs = write_sorted_runs(
            [Ok(batch)], schema, &[SortKey::ascending(Expr::Column("n".into()))],
            &dir, 8, &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 4);
        for path in runs.files() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 8).unwrap();
            let rows = reader.next_batch().unwrap().unwrap().rows().unwrap();
            let values: Vec<_> = rows.into_iter().map(|row| row[0].clone()).collect();
            assert!(values.windows(2).all(|w| w[0].sql_compare(&w[1]).unwrap().unwrap().is_le()));
        }
        let paths = runs.files().to_vec();
        drop(runs);
        assert!(paths.iter().all(|p| !p.exists()));
        fs::remove_dir(&dir).unwrap();
    }
}

#[cfg(test)]
mod sorted_run_bound_checks {
    use super::*;

    #[test]
    fn invalid_run_sizes_fail_before_reading_input() {
        let schema = Schema::new(vec![]).unwrap();
        let input = Vec::<Result<Batch>>::new();
        let keys = [];
        let directory = std::env::temp_dir();
        let control = QueryControl::default();
        assert!(write_sorted_runs(input.clone(), schema.clone(), &keys,
            &directory, 0, &control).is_err());
        assert!(write_sorted_runs(input, schema, &keys,
            &directory, 65_537, &control).is_err());
    }
}

#[cfg(test)]
mod sorted_run_key_prevalidation_tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, schema::{DataType, Field}, scalar::Value};
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn invalid_keys_do_not_poll_any_source_batches() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let calls = Rc::new(Cell::new(0));
        let count = Rc::clone(&calls);
        let source = std::iter::repeat_with(move || {
            count.set(count.get() + 1);
            Err(Error::Execution("source polled".into()))
        });
        let directory = std::env::temp_dir();
        let control = QueryControl::default();
        assert!(write_sorted_runs(source, schema.clone(), &[
            SortKey::ascending(Expr::Column("missing".into()))
        ], &directory, 4, &control).is_err());
        assert_eq!(calls.get(), 0);
        let computed = SortKey::ascending(Expr::binary(
            Expr::Column("n".into()), BinaryOp::Add,
            Expr::Literal(Value::Integer(1)),
        ));
        assert!(write_sorted_runs(
            Vec::<Result<Batch>>::new(), schema, &[computed], &directory, 4, &control
        ).is_err());
    }
}

#[cfg(test)]
mod coalesced_sorted_runs_regressions {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field}};
    use crate::scale::spill::SpillReader;
    use std::{fs, io::BufReader};

    #[test]
    fn one_row_batches_share_bounded_runs_and_keep_all_values() {
        let dir = std::env::temp_dir().join(format!(
            "native-sort-coalesce-{}-{}", std::process::id(),
            RUN_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = (0..27).rev().map(|n| {
            Ok(Batch::from_rows(schema.clone(), vec![vec![Value::Integer(n)]]).unwrap())
        });
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let runs = write_sorted_runs(input, schema.clone(), &keys, &dir, 8,
            &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 4);
        let mut all = Vec::new();
        for path in runs.files() {
            let mut reader = SpillReader::new(BufReader::new(File::open(path).unwrap()), 8).unwrap();
            let rows = reader.next_batch().unwrap().unwrap().rows().unwrap();
            assert!(rows.windows(2).all(|pair| pair[0][0].sql_compare(&pair[1][0])
                .unwrap().unwrap().is_le()));
            all.extend(rows.into_iter().map(|row| row[0].clone()));
        }
        all.sort_by(|a, b| a.sql_compare(b).unwrap().unwrap());
        assert_eq!(all, (0..27).map(Value::Integer).collect::<Vec<_>>());
        drop(runs);
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
        fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn mixed_large_and_empty_batches_never_generate_empty_runs() {
        let dir = std::env::temp_dir().join(format!(
            "native-sort-mixed-{}-{}", std::process::id(),
            RUN_ID.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = [
            Ok(Batch::empty(schema.clone())),
            Ok(Batch::from_rows(schema.clone(),
                (0..13).map(|n| vec![Value::Integer(n)]).collect()).unwrap()),
            Ok(Batch::from_rows(schema.clone(), vec![vec![Value::Integer(99)]]).unwrap()),
        ];
        let runs = write_sorted_runs(input, schema, &[SortKey::ascending(Expr::Column("n".into()))],
            &dir, 5, &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 3);
        drop(runs);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod owned_sorted_spill_run_regressions {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field}};
    use std::{fs, io::BufReader};
    use crate::scale::spill::SpillReader;

    #[test]
    fn moves_large_text_values_without_changing_stable_sort_order() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-sort-owned-{}-{}", std::process::id(),
            RUN_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, true),
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(2), Value::Text("🙂".repeat(4096))],
            vec![Value::Null, Value::Text("before".into())],
            vec![Value::Integer(2), Value::Text("after".into())],
            vec![Value::Integer(1), Value::Null],
        ]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let expected = sort::sort(&batch, &keys).unwrap();
        let runs = write_sorted_runs([Ok(batch)], schema, &keys,
            &dir, 16, &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 1);
        let mut reader = SpillReader::new(BufReader::new(
            std::fs::File::open(&runs.files()[0]).unwrap()), 16).unwrap();
        assert_eq!(reader.next_batch().unwrap().unwrap(), expected);
        assert!(reader.next_batch().unwrap().is_none());
        let paths = runs.files().to_vec();
        drop(runs);
        assert!(paths.iter().all(|path| !path.exists()));
        fs::remove_dir(dir).unwrap();
    }
}
