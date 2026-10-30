//! Generate independently sorted spill runs with deterministic scratch cleanup.
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, schema::Schema, sort::{self, SortKey}, Error, Result};
use super::{cancel::QueryControl, spill::SpillWriter};

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

pub fn write_sorted_runs<I>(
    input: I, schema: Schema, keys: &[SortKey],
    directory: &Path, max_rows_per_run: usize, control: &QueryControl
) -> Result<RunSet>
where I: IntoIterator<Item = Result<Batch>>
{
    if !(1..=65_536).contains(&max_rows_per_run) {
        return Err(Error::Execution("sort run size must be in 1..=65536".into()));
    }
    if keys.is_empty() {
        return Err(Error::Execution("external sort needs at least one key".into()));
    }
    for key in keys {
        if !matches!(&key.expression, crate::expr::Expr::Column(_)) {
            return Err(Error::Unsupported("external merge requires column sort keys".into()));
        }
        key.expression.output_type(&schema)?;
    }
    let mut runs = RunSet { files: Vec::new(), schema: schema.clone() };
    let mut input = input.into_iter();
    loop {
        control.check()?;
        let Some(incoming) = input.next() else { break };
        let incoming = incoming?;
        if incoming.schema() != &schema {
            return Err(Error::Schema("external sort input schema changed".into()));
        }
        let mut start = 0;
        while start < incoming.num_rows() {
            control.check()?;
            let end = start.saturating_add(max_rows_per_run).min(incoming.num_rows());
            let sorted = sort::sort_range(&incoming, keys, start, end)?;
            start = end;
            if sorted.num_rows() == 0 { continue; }
            let id = RUN_ID.fetch_add(1, Ordering::Relaxed);
            let filename = format!("rustspark-sort-{}-{id}.rspill", std::process::id());
            let path = directory.join(filename);
            let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
            runs.files.push(path);
            let mut writer = SpillWriter::new(BufWriter::new(file), schema.clone())?;
            writer.write_batch(&sorted)?;
            writer.finish()?;
        }
    }
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
mod cancelled_source_poll_sort_tests {
    use super::*;
    use crate::{expr::Expr, schema::{DataType, Field}};

    #[test]
    fn cancelled_sort_does_not_poll_source() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let control = QueryControl::default();
        control.cancel();
        let source = std::iter::repeat_with(|| -> Result<Batch> { panic!("source polled") });
        let result = write_sorted_runs(source, schema, &keys,
            std::env::temp_dir().as_path(), 8, &control);
        assert!(matches!(result, Err(Error::Execution(_))));
    }
}
