//! Generate independently sorted spill runs with deterministic scratch cleanup.
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use crate::{batch::Batch, schema::Schema, sort::{self, SortKey}, Error, Result};
use super::{cancel::QueryControl, chunks::BatchChunks, spill::SpillWriter};

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
    if max_rows_per_run == 0 {
        return Err(Error::Execution("sort run size must be positive".into()));
    }
    let mut runs = RunSet { files: Vec::new(), schema: schema.clone() };
    for incoming in input {
        control.check()?;
        let incoming = incoming?;
        if incoming.schema() != &schema {
            return Err(Error::Schema("external sort input schema changed".into()));
        }
        for chunk in BatchChunks::new(&incoming, max_rows_per_run)? {
            control.check()?;
            let sorted = sort::sort(&chunk?, keys)?;
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
