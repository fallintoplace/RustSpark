//! External merge sort over Arrow IPC runs, independent from DataFusion.
use std::{cmp::Ordering, path::Path};

use crate::{
    batch::Batch,
    sort::{self, SortKey},
    spill_ipc::{self, SpillFile, SpillReader},
    schema::Schema,
    Error, Result,
};

struct Run {
    _spill: SpillFile,
    reader: SpillReader,
    batch: Option<Batch>,
    position: usize,
}

pub struct ExternalSort {
    schema: Schema,
    keys: Vec<SortKey>,
    runs: Vec<Run>,
    output_rows: usize,
    finished: bool,
}

/// Sort incoming batches into disk runs and merge them lazily.
/// Each run holds at most run_rows, and output is produced in output_rows
/// batches rather than being collected into one in-memory table.
pub fn external_sort(
    input: impl IntoIterator<Item = Batch>,
    schema: &Schema,
    keys: &[SortKey],
    spill_dir: impl AsRef<Path>,
    run_rows: usize,
    output_rows: usize,
) -> Result<ExternalSort> {
    if run_rows == 0 || output_rows == 0 || run_rows > 65_536 || output_rows > 65_536 {
        return Err(Error::Execution("external sort batch sizes must be in 1..=65536".into()));
    }
    let mut runs = Vec::new();
    for input_batch in input {
        if input_batch.schema() != schema {
            return Err(Error::Schema("external sort schema mismatch".into()));
        }
        for start in (0..input_batch.num_rows()).step_by(run_rows) {
            if runs.len() >= 256 {
                return Err(Error::Execution("too many external sort runs".into()));
            }
            let end = start.saturating_add(run_rows).min(input_batch.num_rows());
            let batch = input_batch.take(&(start..end).collect::<Vec<_>>())?;
            let sorted = sort::sort(&batch, keys)?;
            let spill = spill_ipc::spill(spill_dir.as_ref(), schema, &[sorted])?;
            let mut reader = spill.read()?;
            let first = reader.next().transpose()?;
            runs.push(Run { _spill: spill, reader, batch: first, position: 0 });
        }
    }
    Ok(ExternalSort {
        schema: schema.clone(),
        keys: keys.to_vec(),
        runs,
        output_rows,
        finished: false,
    })
}

fn compare_runs(left: &Run, right: &Run, keys: &[SortKey]) -> Result<Ordering> {
    let a = left.batch.as_ref().ok_or_else(|| Error::Execution("empty sort run".into()))?;
    let b = right.batch.as_ref().ok_or_else(|| Error::Execution("empty sort run".into()))?;
    for key in keys {
        let x = key.expression.evaluate(a, left.position)?;
        let y = key.expression.evaluate(b, right.position)?;
        let order = crate::sort::compare_values(&x, &y, key.descending, key.nulls_first);
        if order != Ordering::Equal { return Ok(order); }
    }
    Ok(Ordering::Equal)
}

impl Iterator for ExternalSort {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        let mut rows = Vec::with_capacity(self.output_rows.min(65_536));
        while rows.len() < self.output_rows {
            let mut best: Option<usize> = None;
            for i in 0..self.runs.len() {
                if self.runs[i].batch.is_none() { continue; }
                if let Some(current) = best {
                    match compare_runs(&self.runs[i], &self.runs[current], &self.keys) {
                        Ok(Ordering::Less) => best = Some(i),
                        Ok(_) => {}
                        Err(e) => {
                            self.finished = true;
                            return Some(Err(e));
                        }
                    }
                } else {
                    best = Some(i);
                }
            }
            let Some(selected) = best else {
                self.finished = true;
                break;
            };
            let run = &mut self.runs[selected];
            let batch = run.batch.as_ref().expect("run exists");
            match batch.row(run.position) {
                Ok(row) => rows.push(row),
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
            run.position += 1;
            if run.position == batch.num_rows() {
                match run.reader.next().transpose() {
                    Ok(next) => {
                        run.batch = next;
                        run.position = 0;
                    }
                    Err(e) => {
                        self.finished = true;
                        return Some(Err(e));
                    }
                }
            }
        }
        if rows.is_empty() {
            None
        } else {
            Some(Batch::from_rows(self.schema.clone(), rows))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::Expr,
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn rejects_excessive_batch_allocations_before_creating_files() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let dir = std::env::temp_dir();
        assert!(external_sort(Vec::<Batch>::new(), &schema, &[], &dir, 65_537, 1).is_err());
        assert!(external_sort(Vec::<Batch>::new(), &schema, &[], &dir, 1, 65_537).is_err());
    }

    #[test]
    fn merge_runs_matches_single_batch_sort() {
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, true)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(10)], vec![Value::Integer(2)],
            vec![Value::Null], vec![Value::Integer(1)],
            vec![Value::Integer(5)], vec![Value::Integer(3)],
        ]).unwrap();
        let keys = vec![SortKey::ascending(Expr::Column("key".into()))];
        let expected = sort::sort(&input, &keys).unwrap().rows().unwrap();
        let merged = external_sort(
            vec![input], &schema, &keys, std::env::temp_dir(), 2, 3
        ).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(merged.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>(), expected);
    }
}

#[cfg(test)]
mod output_allocation_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn empty_input_accepts_large_output_batch_setting() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        let mut output = external_sort(Vec::<Batch>::new(), &schema, &[],
            std::env::temp_dir(), 1, usize::MAX).unwrap();
        assert!(output.next().is_none());
    }
}
