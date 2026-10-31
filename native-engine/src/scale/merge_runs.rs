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
const MAX_OUTPUT_BATCH_BYTES: usize = 16 * 1024 * 1024;

enum AppendStatus { Added, Full, Exhausted }

struct InputRun {
    reader: SpillReader<BufReader<File>>,
    batch: Option<Batch>,
    position: usize,
    exhausted: bool,
}

impl InputRun {
    fn fill(&mut self) -> Result<()> {
        while !self.exhausted &&
            self.batch.as_ref().is_none_or(|batch| self.position >= batch.num_rows()) {
            match self.reader.next_batch()? {
                Some(batch) => {
                    self.batch = Some(batch);
                    self.position = 0;
                }
                None => {
                    self.batch = None;
                    self.exhausted = true;
                }
            }
        }
        Ok(())
    }

    fn peek(&self, column: usize) -> Option<&Value> {
        let batch = self.batch.as_ref()?;
        batch.column(column).ok()?.get(self.position)
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

fn bind_sort_keys(schema: &Schema, keys: &[SortKey]) -> Result<Vec<Key>> {
    if keys.is_empty() {
        return Err(Error::Execution("external merge needs at least one sort key".into()));
    }
    let mut selected = Vec::with_capacity(keys.len());
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
    Ok(selected)
}

pub struct RunMerger {
    runs: Vec<InputRun>,
    schema: Schema,
    keys: Vec<Key>,
    output_batch_rows: usize,
    control: QueryControl,
    // Indices of active runs, ordered as a stable min-heap by current key.
    heap: Vec<usize>,
    heap_ready: bool,
    // The root is reloaded and reinserted at the start of the next append.
    // This keeps I/O failures from discarding the row already emitted.
    pending_refill: Option<usize>,
    pending_error: Option<Error>,
    stopped: bool,
}

impl RunMerger {
    pub fn new(
        paths: &[impl AsRef<Path>], schema: Schema, keys: &[SortKey],
        input_batch_rows: usize, output_batch_rows: usize, control: QueryControl
    ) -> Result<Self> {
        if paths.len() > MAX_FAN_IN || input_batch_rows == 0 || output_batch_rows == 0 ||
            input_batch_rows > 65_536 || output_batch_rows > 65_536 {
            return Err(Error::Execution("invalid external sort fan-in or batch size".into()));
        }
        let selected = bind_sort_keys(&schema, keys)?;
        let mut runs = Vec::new();
        for path in paths {
            let reader = SpillReader::new(
                BufReader::new(File::open(path)?), input_batch_rows)?;
            if reader.schema() != &schema {
                return Err(Error::Schema("external sort run schema mismatch".into()));
            }
            runs.push(InputRun {
                reader, batch: None, position: 0, exhausted: false
            });
        }
        Ok(Self {
            runs, schema, keys: selected, output_batch_rows, control,
            heap: Vec::new(), heap_ready: false,
            pending_refill: None, pending_error: None, stopped: false
        })
    }

    fn less_run(&self, a: usize, b: usize) -> bool {
        for key in &self.keys {
            let left = self.runs[a].peek(key.column).expect("active run has a key");
            let right = self.runs[b].peek(key.column).expect("active run has a key");
            match compare_value(left, right, key) {
                Ordering::Less => return true,
                Ordering::Greater => return false,
                Ordering::Equal => {}
            }
        }
        a < b // Stable ties retain the input-run order.
    }

    fn sift_down(&mut self, mut root: usize) {
        let len = self.heap.len();
        loop {
            let left = root * 2 + 1;
            if left >= len { break; }
            let right = left + 1;
            let mut smallest = left;
            if right < len && self.less_run(self.heap[right], self.heap[left]) {
                smallest = right;
            }
            if !self.less_run(self.heap[smallest], self.heap[root]) { break; }
            self.heap.swap(root, smallest);
            root = smallest;
        }
    }

    fn append_next(&mut self, output: &mut [Vec<Value>],
        output_bytes: &mut usize, output_has_rows: bool) -> Result<AppendStatus> {
        self.control.check()?;
        if !self.heap_ready {
            for run in &mut self.runs { run.fill()?; }
            self.heap = (0..self.runs.len())
                .filter(|&index| self.runs[index].peek(self.keys[0].column).is_some()).collect();
            for parent in (0..self.heap.len() / 2).rev() {
                self.sift_down(parent);
            }
            self.heap_ready = true;
        }
        if let Some(index) = self.pending_refill.take() {
            debug_assert_eq!(self.heap.first().copied(), Some(index));
            let selected = &mut self.runs[index];
            selected.fill()?;
            if selected.peek(self.keys[0].column).is_none() {
                self.heap.swap_remove(0);
            }
            if !self.heap.is_empty() { self.sift_down(0); }
        }
        let Some(&index) = self.heap.first() else { return Ok(AppendStatus::Exhausted); };
        let selected = &mut self.runs[index];
        let batch = selected.batch.as_ref()
            .ok_or_else(|| Error::Execution("missing active external merge batch".into()))?;
        // Count retained Value slots and text payloads before copying any
        // row. A row that cannot fit by itself is an execution error.
        let mut row_bytes = self.schema.len().saturating_mul(std::mem::size_of::<Value>());
        for column in 0..self.schema.len() {
            if let Value::Text(text) = &batch.column(column)?[selected.position] {
                row_bytes = row_bytes.saturating_add(text.len());
            }
        }
        if row_bytes > MAX_OUTPUT_BATCH_BYTES {
            return Err(Error::Execution("external merge row exceeds output byte budget".into()));
        }
        if output_has_rows && output_bytes.saturating_add(row_bytes) > MAX_OUTPUT_BATCH_BYTES {
            // Do not advance the heap or input cursor. The row will be
            // emitted at the start of the following output batch.
            return Ok(AppendStatus::Full);
        }
        *output_bytes = output_bytes.saturating_add(row_bytes);
        for (column, dest) in output.iter_mut().enumerate() {
            dest.push(batch.column(column)?[selected.position].clone());
        }
        selected.position += 1;
        // Refill and rebalance on the next append, before any further row is
        // copied. A corrupt later spill batch cannot hide a valid prefix.
        self.pending_refill = Some(index);
        Ok(AppendStatus::Added)
    }
}

impl Iterator for RunMerger {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        if let Some(error) = self.pending_error.take() {
            self.stopped = true;
            return Some(Err(error));
        }
        // Reserve common small batches without preallocating a full large output budget.
        let capacity = self.output_batch_rows.min(256);
        let mut columns: Vec<Vec<Value>> =
            (0..self.schema.len()).map(|_| Vec::with_capacity(capacity)).collect();
        let mut rows = 0usize;
        let mut output_bytes = 0usize;
        while rows < self.output_batch_rows {
            match self.append_next(&mut columns, &mut output_bytes, rows != 0) {
                Ok(AppendStatus::Added) => rows += 1,
                Ok(AppendStatus::Full) => break,
                Ok(AppendStatus::Exhausted) => {
                    self.stopped = true;
                    break;
                }
                Err(error) => {
                    if rows == 0 {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                    // Preserve successfully emitted rows and report the
                    // terminal I/O, deadline, or cancellation error next.
                    self.pending_error = Some(error);
                    break;
                }
            }
        }
        if rows == 0 { None }
        else { Some(Batch::from_validated_columns(self.schema.clone(), columns, rows)) }
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

#[cfg(test)]
mod empty_sort_key_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn rejects_unordered_merges_without_opening_files() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        assert!(RunMerger::new(&[] as &[&str], schema, &[], 4, 4,
            QueryControl::default()).is_err());
    }
}

#[cfg(test)]
mod consolidated_merge_batch_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn reject_large_merge_buffers_before_accessing_files() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let files: [&str; 0] = [];
        assert!(RunMerger::new(&files, schema.clone(), &[], 1, 65_537, QueryControl::default()).is_err());
        assert!(RunMerger::new(&files, schema, &[], 65_537, 1, QueryControl::default()).is_err());
    }
}

#[cfg(test)]
mod selective_spill_refill_perf_tests {
    use super::*;
    use crate::{scale::sorted_runs::write_sorted_runs, schema::{DataType, Field}};
    use std::fs;

    #[test]
    fn uneven_runs_with_duplicate_keys_merge_without_losing_rows() {
        let directory = std::env::temp_dir().join(format!(
            "native-merge-perf-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&directory).unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema.clone(),
            [9, 3, 3, 1, 8, 2, 9, 5, 3, 7, 0].into_iter()
                .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let runs = write_sorted_runs([Ok(input.clone())], schema.clone(), &keys,
            &directory, 2, &QueryControl::default()).unwrap();
        let result = RunMerger::new(runs.files(), schema, &keys, 1, 3, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        let actual = result.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(actual, crate::sort::sort(&input, &keys).unwrap().rows().unwrap());
        drop(runs);
        fs::remove_dir(directory).unwrap();
    }
}

#[cfg(test)]
mod heap_merge_many_runs_tests {
    use super::*;
    use crate::{scale::sorted_runs::write_sorted_runs, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn merges_forty_eight_runs_with_ties_and_small_io_batches() {
        let dir = std::env::temp_dir().join(format!("native-heap-merge-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("key", DataType::Integer, false), Field::new("id", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..192).rev().map(|n|
            vec![Value::Integer(n % 17), Value::Integer(n)]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let runs = write_sorted_runs([Ok(batch)], schema.clone(), &keys, &dir, 4,
            &QueryControl::default()).unwrap();
        assert_eq!(runs.files().len(), 48);
        let merged = RunMerger::new(runs.files(), schema, &keys, 3, 7,
            QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = merged.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows.len(), 192);
        assert!(rows.windows(2).all(|pair| matches!(
            (&pair[0][0], &pair[1][0]),
            (Value::Integer(a), Value::Integer(b)) if a <= b
        )));
        let mut ids = rows.into_iter().map(|r| match r[1] { Value::Integer(n) => n, _ => unreachable!() }).collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (0..192).collect::<Vec<_>>());
        drop(runs);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod columnar_spill_cursor_tests {
    use super::*;
    use crate::{scale::sorted_runs::write_sorted_runs, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn nullable_text_keys_merge_without_per_row_buffers() {
        let dir = std::env::temp_dir().join(format!("native-column-merge-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false), Field::new("tag", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..40).rev().map(|n| vec![Value::Integer(n),
            if n % 7 == 0 { Value::Null } else { Value::Text(format!("group-{}", n % 5)) }
        ]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("tag".into()))];
        let expected = crate::sort::sort(&batch, &keys).unwrap();
        let runs = write_sorted_runs([Ok(batch)], schema.clone(), &keys, &dir, 3, &QueryControl::default()).unwrap();
        let merged = RunMerger::new(runs.files(), schema, &keys, 2, 4, QueryControl::default()).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(merged.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>(), expected.rows().unwrap());
        drop(runs);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod columnar_spill_output_tests {
    use super::*;
    use crate::{scale::sorted_runs::write_sorted_runs, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering as AtomicOrdering}};
    static ID: AtomicU64 = AtomicU64::new(1);
    #[test]
    fn multi_key_merge_directly_emits_stable_columnar_batches() {
        let dir = std::env::temp_dir().join(format!("native-merge-output-{}-{}",
            std::process::id(), ID.fetch_add(1, AtomicOrdering::Relaxed)));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![Field::new("group", DataType::Integer, false),
            Field::new("tag", DataType::Text, true), Field::new("payload", DataType::Text, false)]).unwrap();
        let source = Batch::from_rows(schema.clone(), (0..173).rev().map(|n| vec![
            Value::Integer(n % 13),
            if n % 17 == 0 { Value::Null } else { Value::Text(format!("key-{}", n % 5)) },
            Value::Text(format!("row-{n}-{}", "long".repeat(40)))
        ]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("group".into())),
            SortKey::ascending(Expr::Column("tag".into()))];
        let expected = crate::sort::sort(&source, &keys).unwrap();
        let runs = write_sorted_runs([Ok(source)], schema.clone(), &keys, &dir, 9,
            &QueryControl::default()).unwrap();
        let merged = RunMerger::new(runs.files(), schema, &keys, 3, 7,
            QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert!(merged.iter().all(|b| b.num_rows() <= 7));
        assert_eq!(merged.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>(), expected.rows().unwrap());
        drop(runs);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod external_merge_output_byte_budget_regressions {
    use super::*;
    use crate::{scale::sorted_runs::write_sorted_runs, schema::{DataType, Field}};
    use std::fs;

    #[test]
    fn large_payloads_split_batches_without_skipping_heap_rows() {
        let dir = std::env::temp_dir().join(format!("native-merge-output-{}-{}",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let schema = Schema::new(vec![
            Field::new("key", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let input = Batch::from_rows(schema.clone(), (0..5).rev().map(|n| vec![
            Value::Integer(n), Value::Text(format!("{n}{}", "x".repeat(4 * 1024 * 1024))),
        ]).collect()).unwrap();
        let keys = [SortKey::ascending(Expr::Column("key".into()))];
        let source = write_sorted_runs([Ok(input)], schema.clone(), &keys, &dir, 1,
            &QueryControl::default()).unwrap();
        let output = RunMerger::new(source.files(), schema, &keys, 1, 5,
            QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 2]);
        let values = output.iter().flat_map(|batch| batch.column(0).unwrap())
            .cloned().collect::<Vec<_>>();
        assert_eq!(values, (0..5).map(Value::Integer).collect::<Vec<_>>());
        drop(source);
        fs::remove_dir(dir).unwrap();
    }
}

#[cfg(test)]
mod deferred_external_merge_error_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field}};
    use super::super::spill::SpillWriter;
    use std::{fs::{self, OpenOptions}, io::{BufWriter, Write}};

    #[test]
    fn corrupt_later_batch_preserves_valid_prefix_then_reports_io_error() {
        let path = std::env::temp_dir().join(format!(
            "native-merge-corrupt-{}-{}.rspill", std::process::id(),
            std::time::SystemTime::now().duration_since(
                std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        let writer = SpillWriter::new(
            BufWriter::new(fs::File::create(&path).unwrap()), schema.clone(),
        ).unwrap();
        let mut writer = writer;
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();
        OpenOptions::new().append(true).open(&path).unwrap()
            .write_all(&[0xff]).unwrap();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let mut merge = RunMerger::new(&[&path], schema, &keys, 2, 8,
            QueryControl::default()).unwrap();
        assert_eq!(merge.next().unwrap().unwrap().rows().unwrap(),
            batch.rows().unwrap());
        assert!(merge.next().unwrap().unwrap_err().to_string().contains("io"));
        assert!(merge.next().is_none());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn cancelled_merge_reports_error_after_partial_valid_output() {
        let path = std::env::temp_dir().join(format!(
            "native-merge-cancel-{}-{}.rspill", std::process::id(),
            std::time::SystemTime::now().duration_since(
                std::time::UNIX_EPOCH).unwrap().as_nanos(),
        ));
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        let mut writer = SpillWriter::new(
            BufWriter::new(fs::File::create(&path).unwrap()), schema.clone(),
        ).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();
        let control = QueryControl::default();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let mut merge = RunMerger::new(&[&path], schema, &keys, 2, 1,
            control.clone()).unwrap();
        assert_eq!(merge.next().unwrap().unwrap().num_rows(), 1);
        control.cancel();
        assert!(merge.next().unwrap().is_err());
        assert!(merge.next().is_none());
        fs::remove_file(path).unwrap();
    }
}
