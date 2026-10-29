//! Streaming physical execution of the supported native logical-plan subset.
use std::{collections::HashSet, path::PathBuf};
use crate::{
    batch::Batch, plan::Plan, project::Selection, scalar::Value,
    schema::{Field, Schema}, Error, Result,
};
use super::{
    cancel::QueryControl,
    chunks::RowBatcher, file_catalog::FileCatalog,
    stream_ops::{FilterStream, ProjectStream, LimitStream},
    union_stream::UnionAllStream,
    sorted_runs::write_sorted_runs,
    merge_passes::{merge_to_fan_in, MultiPassRuns},
    merge_runs::RunMerger,
};

pub type BatchStream = Box<dyn Iterator<Item = Result<Batch>> + Send>;

// The merge reader must be dropped before its owned scratch files.
struct SortedOutput {
    reader: RunMerger,
    _files: MultiPassRuns,
}

impl Iterator for SortedOutput {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> { self.reader.next() }
}

const MAX_STREAM_DISTINCT_KEYS: usize = 200_000;
const MAX_STREAM_DISTINCT_BYTES: usize = 64 * 1024 * 1024;

/// Keep first occurrences across batch boundaries within an explicit memory budget.
struct DistinctOutput {
    source: BatchStream,
    control: QueryControl,
    seen: HashSet<Vec<Value>>,
    retained_bytes: usize,
    max_keys: usize,
    max_bytes: usize,
    stopped: bool,
}

impl DistinctOutput {
    fn new(source: BatchStream, control: QueryControl, max_keys: usize, max_bytes: usize) -> Self {
        Self {
            source, control, seen: HashSet::new(), retained_bytes: 0,
            max_keys, max_bytes, stopped: false,
        }
    }
}

impl Iterator for DistinctOutput {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let batch = match self.source.next()? {
                Ok(batch) => batch,
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            };
            let mut selected = Vec::new();
            for row in 0..batch.num_rows() {
                let key = match batch.row(row) {
                    Ok(key) => key,
                    Err(error) => {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                };
                if self.seen.contains(&key) { continue; }
                let footprint = key.iter().fold(
                    64usize.saturating_add(key.len().saturating_mul(std::mem::size_of::<Value>())),
                    |total, value| match value {
                        Value::Text(text) => total.saturating_add(text.len()),
                        _ => total,
                    },
                );
                let next_total = self.retained_bytes.checked_add(footprint);
                if self.seen.len() >= self.max_keys ||
                    next_total.is_none_or(|size| size > self.max_bytes) {
                    self.stopped = true;
                    return Some(Err(Error::Execution("stream DISTINCT memory budget exceeded".into())));
                }
                self.retained_bytes = next_total.expect("validated DISTINCT memory");
                self.seen.insert(key);
                selected.push(row);
            }
            if !selected.is_empty() {
                return Some(batch.take(&selected));
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamOptions {
    pub batch_rows: usize,
    pub max_plan_depth: usize,
    /// Scratch directory for external sort and aggregate operators.
    pub scratch_directory: PathBuf,
    pub sort_run_rows: usize,
    pub merge_fan_in: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            batch_rows: 4096, max_plan_depth: 64,
            scratch_directory: std::env::temp_dir(),
            sort_run_rows: 4096, merge_fan_in: 16,
        }
    }
}

fn infer(
    plan: &Plan, catalog: &FileCatalog, depth: usize, max_depth: usize
) -> Result<Schema> {
    if depth >= max_depth {
        return Err(Error::Execution("streaming query exceeds plan depth budget".into()));
    }
    let child = |plan: &Plan| infer(plan, catalog, depth + 1, max_depth);
    match plan {
        Plan::Scan { table } => Ok(catalog.get(table)?.schema().clone()),
        Plan::Filter { input, .. } |
        Plan::Sort { input, .. } |
        Plan::Limit { input, .. } |
        Plan::Distinct { input } => child(input),
        Plan::Project { input, selections } => {
            let source = child(input)?;
            let fields = selections.iter().map(|selection: &Selection| {
                Ok(Field::new(
                    &selection.alias, selection.expression.output_type(&source)?, true
                ))
            }).collect::<Result<Vec<_>>>()?;
            Schema::new(fields)
        }
        Plan::UnionAll { left, right } => {
            let left_schema = child(left)?;
            if left_schema != child(right)? {
                return Err(Error::Schema("UNION ALL inputs have different schemas".into()));
            }
            Ok(left_schema)
        }
        _ => Err(Error::Unsupported(
            "streaming schema inference is unavailable for this operator".into(),
        )),
    }
}

pub fn output_schema(
    plan: &Plan, catalog: &FileCatalog, options: &StreamOptions
) -> Result<Schema> {
    infer(plan, catalog, 0, options.max_plan_depth)
}

fn lower(
    plan: &Plan, catalog: &FileCatalog, options: &StreamOptions,
    control: &QueryControl, depth: usize
) -> Result<BatchStream> {
    if depth >= options.max_plan_depth {
        return Err(Error::Execution("streaming plan exceeds depth budget".into()));
    }
    control.check()?;
    let child = |plan: &Plan| lower(plan, catalog, options, control, depth + 1);
    match plan {
        Plan::Scan { table } => {
            let source = catalog.get(table)?;
            let reader = source.open()?;
            let batches = RowBatcher::new(reader, source.schema().clone(), options.batch_rows)?;
            Ok(Box::new(batches))
        }
        Plan::Filter { input, predicate } =>
            Ok(Box::new(FilterStream::new(child(input)?, predicate.clone(), control.clone()))),
        Plan::Distinct { input } => Ok(Box::new(DistinctOutput::new(
            child(input)?, control.clone(), MAX_STREAM_DISTINCT_KEYS, MAX_STREAM_DISTINCT_BYTES
        ))),
        Plan::Project { input, selections } =>
            Ok(Box::new(ProjectStream::new(child(input)?, selections.clone(), control.clone()))),
        Plan::Limit { input, offset, count } =>
            Ok(Box::new(LimitStream::new(child(input)?, *offset, *count, control.clone()))),
        Plan::Sort { input, keys } => {
            let schema = infer(input, catalog, depth + 1, options.max_plan_depth)?;
            for key in keys {
                let crate::expr::Expr::Column(name) = &key.expression else {
                    return Err(Error::Unsupported("external sort requires column keys".into()));
                };
                schema.column(name)?;
            }
            let runs = write_sorted_runs(
                child(input)?, schema.clone(), keys, &options.scratch_directory,
                options.sort_run_rows, control
            )?;
            let files = merge_to_fan_in(
                runs, &options.scratch_directory, schema.clone(), keys,
                options.merge_fan_in, options.sort_run_rows, control
            )?;
            let reader = RunMerger::new(
                files.files(), schema, keys,
                options.sort_run_rows, options.batch_rows, control.clone()
            )?;
            Ok(Box::new(SortedOutput { reader, _files: files }))
        }
        Plan::UnionAll { left, right } => {
            let schema = infer(plan, catalog, depth, options.max_plan_depth)?;
            Ok(Box::new(UnionAllStream::new(
                child(left)?, child(right)?, schema, control.clone()
            )))
        }
        _ => Err(Error::Unsupported(
            "operator has no streaming physical implementation yet".into()
        )),
    }
}

pub fn execute(
    plan: &Plan, catalog: &FileCatalog,
    options: &StreamOptions, control: QueryControl
) -> Result<BatchStream> {
    if options.batch_rows == 0 || options.sort_run_rows == 0 ||
        options.sort_run_rows > 65_536 ||
        !(2..=64).contains(&options.merge_fan_in) || options.max_plan_depth == 0 {
        return Err(Error::Execution("invalid streaming executor settings".into()));
    }
    output_schema(plan, catalog, options)?;
    lower(plan, catalog, options, &control, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}, sql};
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static FILE_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn order_by_uses_multiple_spill_merge_passes() {
        let dir = std::env::temp_dir().join(format!(
            "rustspark-stream-sort-{}-{}",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("sales.csv");
        let mut source = String::from("id,price\n");
        for id in 0..61 {
            source.push_str(&format!("{id},10\n"));
        }
        fs::write(&path, source).unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("price", DataType::Integer, false),
        ]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("sales", &path, schema, 1024).unwrap();
        let plan = sql::parse_sql(
            "SELECT id FROM sales WHERE price >= 1 ORDER BY id DESC LIMIT 3"
        ).unwrap();
        let options = StreamOptions {
            batch_rows: 4,
            sort_run_rows: 5,
            merge_fan_in: 3,
            scratch_directory: dir.clone(),
            ..StreamOptions::default()
        };
        let rows = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap()
            .iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(60)],
            vec![Value::Integer(59)],
            vec![Value::Integer(58)]
        ]);
        let remaining = fs::read_dir(&dir).unwrap().count();
        assert_eq!(remaining, 1, "all external sort files must be cleaned up");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sql_stream_reads_and_filters_csv_across_batches() {
        let path = std::env::temp_dir().join(format!("rustspark-stream-{}-{}.csv",
            std::process::id(), FILE_ID.fetch_add(1, Ordering::Relaxed)));
        fs::write(&path, "id,n\n1,4\n2,10\n3,12\n4,20\n").unwrap();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("n", DataType::Integer, false),
        ]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("sales", &path, schema, 2048).unwrap();
        let plan = sql::parse_sql(
            "SELECT id FROM sales WHERE n >= 10 LIMIT 1 OFFSET 1").unwrap();
        let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
        let output = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        let rows = output.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(3)]]);
        fs::remove_file(path).unwrap();
    }
}


#[cfg(test)]
mod distinct_stream_feature_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn distinct_deduplicates_across_batch_boundaries() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Integer(1)], vec![Value::Null]
        ]).unwrap();
        let second = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)]
        ]).unwrap();
        let source: BatchStream = Box::new(vec![Ok(first), Ok(second)].into_iter());
        let result = DistinctOutput::new(source, QueryControl::default(), 10, 10000)
            .collect::<Result<Vec<_>>>().unwrap();
        let rows = result.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Null], vec![Value::Integer(2)]
        ]);
    }

    #[test]
    fn fails_closed_on_distinct_key_memory_exhaustion() {
        let schema = Schema::new(vec![Field::new("n", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("abc".into())]
        ]).unwrap();
        let source: BatchStream = Box::new(vec![Ok(input)].into_iter());
        let mut distinct = DistinctOutput::new(source, QueryControl::default(), 10, 1);
        assert!(distinct.next().unwrap().is_err());
        assert!(distinct.next().is_none());
    }

    #[test]
    fn executor_runs_native_sql_select_distinct() {
        use std::{fs, time::{SystemTime, UNIX_EPOCH}};
        let dir = std::env::temp_dir();
        let path = dir.join(format!("native-stream-distinct-{}-{}.csv",
            std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        fs::write(&path, "n\n1\n2\n1\n3\n2\n").unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("t", &path, schema, 1024).unwrap();
        let plan = crate::sql::parse_sql("SELECT DISTINCT n FROM t").unwrap();
        let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
        let rows = execute(&plan, &catalog, &options, QueryControl::default()).unwrap()
            .collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]
        ]);
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod sort_prevalidation_tests {
    use super::*;
    use crate::{expr::Expr, sort::SortKey, schema::{DataType, Field}};
    #[test]
    fn rejects_unsupported_sort_before_opening_csv() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("data", "/nonexistent/rustspark-early-validation.csv", schema, 1024).unwrap();
        let plan = Plan::Sort {
            input: Box::new(Plan::scan("data")),
            keys: vec![SortKey::ascending(Expr::Literal(Value::Integer(1)))],
        };
        let result = execute(&plan, &catalog, &StreamOptions::default(), QueryControl::default());
        assert!(matches!(result, Err(Error::Unsupported(_))));
    }
}
