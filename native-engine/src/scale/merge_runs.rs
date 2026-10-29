//! Bounded multiway merge over sorted spill files.
use std::{
    cmp::Ordering,
    fs::File,
    io::BufReader,
    path::Path,
};
use crate::{
    batch::Batch, expr::Expr, scalar::Value, schema::Schema,
    sort::SortKey, Error, Result,
};
use super::{cancel::QueryControl, spill::SpillReader};

const MAX_FAN_IN: usize = 64;

struct InputRun {
    reader: SpillReader<BufReader<File>>,
    rows: Vec<Vec<Value>>,
    position: usize,
    exhausted: bool,
}

impl InputRun {
    fn fill(&mut self) -> Result<()> {
        while !self.exhausted && self.position == self.rows.len() {
            match self.reader.next_batch()? {
                Some(batch) => {
                    self.rows = batch.rows()?;
                    self.position = 0;
                }
                None => {
                    self.rows.clear();
                    self.exhausted = true;
                }
            }
        }
        Ok(())
    }

    fn peek(&self) -> Option<&[Value]> {
        self.rows.get(self.position).map(Vec::as_slice)
    }
}

struct Key {
    column: usize,
    descending: bool,
    nulls_first: bool,
}

fn compare_value(a: &Value, b: &Value, key: &Key) -> Ordering {
    crate::sort::compare_values(a, b, key.descending, key.nulls_first)
}

pub struct RunMerger {
    runs: Vec<InputRun>,
    schema: Schema,
    keys: Vec<Key>,
    output_batch_rows: usize,
    control: QueryControl,
    stopped: bool,
}

impl RunMerger {
    pub fn new(
        paths: &[impl AsRef<Path>], schema: Schema, keys: &[SortKey],
        input_batch_rows: usize, output_batch_rows: usize, control: QueryControl
    ) -> Result<Self> {
        if paths.len() > MAX_FAN_IN || input_batch_rows == 0 || output_batch_rows == 0 {
            return Err(Error::Execution("invalid external sort fan-in or batch size".into()));
        }
        let mut selected = Vec::new();
        for key in keys {
            let Expr::Column(name) = &key.expression else {
                return Err(Error::Unsupported("external merge requires column sort keys".into()));
            };
            selected.push(Key {
                column: schema.column(name)?,
                descending: key.descending,
                nulls_first: key.nulls_first,
            });
        }
        let mut runs = Vec::new();
        for path in paths {
            let reader = SpillReader::new(
                BufReader::new(File::open(path)?), input_batch_rows)?;
            if reader.schema() != &schema {
                return Err(Error::Schema("external sort run schema mismatch".into()));
            }
            runs.push(InputRun {
                reader, rows: Vec::new(), position: 0, exhausted: false
            });
        }
        Ok(Self {
            runs, schema, keys: selected, output_batch_rows, control, stopped: false
        })
    }

    fn less(&self, a: &[Value], b: &[Value]) -> bool {
        for key in &self.keys {
            let ord = compare_value(&a[key.column], &b[key.column], key);
            if ord != Ordering::Equal { return ord == Ordering::Less; }
        }
        false
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        self.control.check()?;
        for run in &mut self.runs { run.fill()?; }
        let mut best = None;
        for (index, run) in self.runs.iter().enumerate() {
            if let Some(row) = run.peek() {
                if best.is_none_or(|old: usize| self.less(row, self.runs[old].peek().unwrap())) {
                    best = Some(index);
                }
            }
        }
        let Some(index) = best else { return Ok(None); };
        let row = self.runs[index].peek().unwrap().to_vec();
        self.runs[index].position += 1;
        Ok(Some(row))
    }
}

impl Iterator for RunMerger {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        let mut rows = Vec::with_capacity(self.output_batch_rows.min(65_536));
        while rows.len() < self.output_batch_rows {
            match self.next_row() {
                Ok(Some(row)) => rows.push(row),
                Ok(None) => {
                    self.stopped = true;
                    break;
                }
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
        if rows.is_empty() { None }
        else { Some(Batch::from_rows(self.schema.clone(), rows)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        scale::sorted_runs::write_sorted_runs, schema::{DataType, Field},
    };
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn globally_orders_multiple_spill_runs() {
        let dir = std::env::temp_dir().join(format!("native-merge-test-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..71).rev()
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let runs = write_sorted_runs([Ok(batch)], schema.clone(), &keys, &dir, 10,
            &QueryControl::default()).unwrap();
        let merger = RunMerger::new(runs.files(), schema, &keys, 10, 7,
            QueryControl::default()).unwrap();
        let output = merger.collect::<Result<Vec<_>>>().unwrap();
        let values: Vec<_> = output.iter().flat_map(|b| b.rows().unwrap()).map(|r| r[0].clone()).collect();
        assert_eq!(values, (0..71).map(Value::Integer).collect::<Vec<_>>());
        drop(runs);
        fs::remove_dir(dir).unwrap();
    }
}
