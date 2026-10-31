//! Streaming physical execution of the supported native logical-plan subset.
use std::{collections::HashMap, fs::File, io::BufReader, path::PathBuf};
use crate::{
    aggregate::{self, AggregateOp}, batch::Batch, expr::Expr, join::{self, JoinKind}, plan::Plan,
    project::Selection, scalar::Value,
    schema::{DataType, Field, Schema}, Error, Result,
};
use super::{
    cancel::QueryControl,
    chunks::RowBatcher, file_catalog::FileCatalog,
    stream_ops::{FilterStream, ProjectStream, LimitStream},
    union_stream::{UnionAllStream, UnionDistinctStream},
    cross_join_stream::CrossJoinStream,
    set_stream::{SetStream, SetMode, set_schema},
    sorted_runs::write_sorted_runs,
    merge_passes::{merge_to_fan_in, MultiPassRuns},
    merge_runs::RunMerger,
    grouped_spill::{grouped_aggregate, GroupFiles},
    grace_join::{grace_join, JoinFiles}, spill::SpillReader,
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

/// The shard file ownership must outlive the active spill reader.
struct GroupedOutput {
    files: GroupFiles,
    reader: Option<SpillReader<BufReader<File>>>,
    next_file: usize,
    batch_rows: usize,
    control: QueryControl,
    stopped: bool,
}

impl Iterator for GroupedOutput {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                self.reader = None;
                return Some(Err(error));
            }
            if let Some(reader) = &mut self.reader {
                match reader.next_batch() {
                    Ok(Some(batch)) if batch.num_rows() == 0 => continue,
                    Ok(Some(batch)) => return Some(Ok(batch)),
                    Ok(None) => self.reader = None,
                    Err(error) => {
                        self.stopped = true;
                        self.reader = None;
                        return Some(Err(error));
                    }
                }
                continue;
            }
            let Some(path) = self.files.paths().get(self.next_file) else {
                self.stopped = true;
                return None;
            };
            self.next_file += 1;
            let next_reader = (|| {
                let reader = SpillReader::new(BufReader::new(File::open(path)?), self.batch_rows)?;
                if reader.schema() != self.files.schema() {
                    return Err(Error::Schema("grouped result spill schema mismatch".into()));
                }
                Ok(reader)
            })();
            match next_reader {
                Ok(reader) => self.reader = Some(reader),
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Keep hash join shard paths owned until the active reader has finished.
struct JoinedOutput {
    files: JoinFiles,
    reader: Option<SpillReader<BufReader<File>>>,
    next_file: usize,
    batch_rows: usize,
    control: QueryControl,
    stopped: bool,
}
impl Iterator for JoinedOutput {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                self.reader = None;
                return Some(Err(error));
            }
            if let Some(reader) = &mut self.reader {
                match reader.next_batch() {
                    Ok(Some(batch)) if batch.num_rows() == 0 => continue,
                    Ok(Some(batch)) => return Some(Ok(batch)),
                    Ok(None) => self.reader = None,
                    Err(error) => {
                        self.stopped = true;
                        self.reader = None;
                        return Some(Err(error));
                    }
                }
                continue;
            }
            let Some(path) = self.files.paths().get(self.next_file) else {
                self.stopped = true;
                return None;
            };
            self.next_file += 1;
            let next_reader = (|| {
                let reader = SpillReader::new(BufReader::new(File::open(path)?), self.batch_rows)?;
                if reader.schema() != self.files.schema() {
                    return Err(Error::Schema("join result spill schema mismatch".into()));
                }
                Ok(reader)
            })();
            match next_reader {
                Ok(reader) => self.reader = Some(reader),
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

const MAX_STREAM_DISTINCT_KEYS: usize = 200_000;
const MAX_STREAM_DISTINCT_BYTES: usize = 64 * 1024 * 1024;

/// Keep first occurrences across batch boundaries within an explicit memory budget.
struct DistinctOutput {
    source: BatchStream,
    control: QueryControl,
    seen: HashMap<Vec<Value>, ()>,
    retained_bytes: usize,
    max_keys: usize,
    max_bytes: usize,
    stopped: bool,
}

impl DistinctOutput {
    fn new(source: BatchStream, control: QueryControl, max_keys: usize, max_bytes: usize) -> Self {
        Self {
            source, control, seen: HashMap::new(), retained_bytes: 0,
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
            let batch = match self.source.next() {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
                None => {
                    // An upstream Iterator need not itself be fused. Once
                    // DISTINCT reports EOF it must never emit later rows.
                    self.stopped = true;
                    return None;
                }
            };
            if batch.schema().is_empty() && batch.num_rows() != 0 {
                let empty_key = Vec::new();
                if self.seen.contains_key(&empty_key) { continue; }
                let next_total = self.retained_bytes.checked_add(64);
                if self.seen.len() >= self.max_keys ||
                    next_total.is_none_or(|size| size > self.max_bytes) {
                    self.stopped = true;
                    return Some(Err(Error::Execution("stream DISTINCT memory budget exceeded".into())));
                }
                self.retained_bytes = next_total.expect("checked DISTINCT memory");
                self.seen.insert(empty_key, ());
                return Some(batch.slice_rows(0, 1));
            }
            let mut selected = Vec::new();
            for row in 0..batch.num_rows() {
                let key = match batch.row(row) {
                    Ok(key) => key,
                    Err(error) => {
                        self.stopped = true;
                        return Some(Err(error));
                    }
                };
                let at_limit = self.seen.len() >= self.max_keys;
                match self.seen.entry(key) {
                    std::collections::hash_map::Entry::Occupied(_) => continue,
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let key = entry.key();
                        let footprint = key.iter().fold(
                            64usize.saturating_add(key.len().saturating_mul(std::mem::size_of::<Value>())),
                            |total, value| match value {
                                Value::Text(text) => total.saturating_add(text.len()),
                                _ => total,
                            },
                        );
                        let next_total = self.retained_bytes.checked_add(footprint);
                        if at_limit || next_total.is_none_or(|size| size > self.max_bytes) {
                            self.stopped = true;
                            return Some(Err(Error::Execution("stream DISTINCT memory budget exceeded".into())));
                        }
                        self.retained_bytes = next_total.expect("validated DISTINCT memory");
                        entry.insert(());
                        selected.push(row);
                    }
                }
            }
            if !selected.is_empty() {
                if selected.len() == batch.num_rows() {
                    return Some(Ok(batch));
                }
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
        Plan::Scan { table } => Ok(catalog.schema(table)?.clone()),
        Plan::Filter { input, predicate } => {
            let schema = child(input)?;
            let kind = predicate.output_type(&schema)?;
            if kind != DataType::Boolean &&
                !matches!(predicate, crate::expr::Expr::Literal(Value::Null)) {
                return Err(Error::Type("stream WHERE predicate must be boolean".into()));
            }
            Ok(schema)
        }
        Plan::Sort { input, keys } => {
            let schema = child(input)?;
            for key in keys {
                key.expression.output_type(&schema)?;
            }
            Ok(schema)
        }
        Plan::Limit { input, .. } |
        Plan::Distinct { input } => child(input),
        Plan::CrossJoin { left, right } => {
            let left_schema = child(left)?;
            let right_schema = child(right)?;
            Ok(join::cross_join(
                &Batch::empty(left_schema), &Batch::empty(right_schema)
            )?.schema().clone())
        }
        Plan::Join { left, right, left_key, right_key, kind } => {
            let left_schema = child(left)?;
            let right_schema = child(right)?;
            Ok(join::hash_join(
                &Batch::empty(left_schema), &Batch::empty(right_schema),
                left_key, right_key, *kind
            )?.schema().clone())
        }
        Plan::Aggregate { input, group_by, aggregates } => {
            if group_by.is_empty() {
                let schema = child(input)?;
                if aggregates.iter().any(|item| matches!(&item.op,
                    AggregateOp::CountDistinct(_) | AggregateOp::SumDistinct(_) |
                    AggregateOp::AvgDistinct(_))) {
                    return Err(Error::Unsupported(
                        "stream global DISTINCT aggregates require a spill path".into()));
                }
                return aggregate::global_output_schema(&schema, aggregates);
            }
            let source = child(input)?;
            let mut fields = Vec::new();
            for (expr, alias) in group_by {
                let Expr::Column(name) = expr else {
                    return Err(Error::Unsupported("spilled GROUP BY requires columns".into()));
                };
                let source_field = source.field(source.column(name)?)?;
                fields.push(Field::new(alias, source_field.data_type, true));
            }
            for item in aggregates {
                let kind = match &item.op {
                    AggregateOp::CountAll | AggregateOp::Count(_) | AggregateOp::CountDistinct(_) =>
                        crate::schema::DataType::Integer,
                    AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr) =>
                        match expr.output_type(&source)? {
                        crate::schema::DataType::Integer | crate::schema::DataType::Float =>
                            crate::schema::DataType::Float,
                        _ => return Err(Error::Type("AVG requires numeric input".into())),
                    },
                    AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr) =>
                        match expr.output_type(&source)? {
                        crate::schema::DataType::Integer => crate::schema::DataType::Integer,
                        crate::schema::DataType::Float => crate::schema::DataType::Float,
                        _ => return Err(Error::Type("SUM requires numeric input".into())),
                    },
                    AggregateOp::Min(expr) | AggregateOp::Max(expr) => expr.output_type(&source)?,
                };
                fields.push(Field::new(&item.alias, kind, true));
            }
            Schema::new(fields)
        }
        Plan::Project { input, selections } => {
            let source = child(input)?;
            let fields = selections.iter().map(|selection: &Selection| {
                Ok(Field::new(
                    &selection.alias, selection.expression.output_type(&source)?, true
                ))
            }).collect::<Result<Vec<_>>>()?;
            Schema::new(fields)
        }
        Plan::Intersect { left, right } |
        Plan::IntersectAll { left, right } |
        Plan::Except { left, right } |
        Plan::ExceptAll { left, right } =>
            set_schema(&child(left)?, &child(right)?),
        Plan::UnionAll { left, right } => {
            let left_schema = child(left)?;
            let right_schema = child(right)?;
            if left_schema.len() != right_schema.len() {
                return Err(Error::Schema("UNION ALL column counts differ".into()));
            }
            let fields = left_schema.fields().iter().zip(right_schema.fields())
                .map(|(left, right)| {
                    if left.data_type != right.data_type {
                        return Err(Error::Type("UNION ALL column types differ".into()));
                    }
                    Ok(Field::new(&left.name, left.data_type,
                        left.nullable || right.nullable))
                }).collect::<Result<Vec<_>>>()?;
            Schema::new(fields)
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
            if let Some(parquet) = catalog.get_parquet(table)? {
                return Ok(Box::new(parquet.open(options.batch_rows)?));
            }
            let source = catalog.get(table)?;
            let reader = source.open()?;
            let batches = RowBatcher::new(reader, source.schema().clone(), options.batch_rows)?;
            Ok(Box::new(batches))
        }
        Plan::Filter { input, predicate } =>
            Ok(Box::new(FilterStream::new(child(input)?, predicate.clone(), control.clone()))),
        Plan::Distinct { input } if matches!(input.as_ref(), Plan::UnionAll { .. }) => {
            let Plan::UnionAll { left, right } = input.as_ref() else {
                unreachable!("checked UNION ALL plan");
            };
            let schema = infer(input, catalog, depth + 1, options.max_plan_depth)?;
            let left = lower(left, catalog, options, control, depth + 2)?;
            let right = lower(right, catalog, options, control, depth + 2)?;
            Ok(Box::new(UnionDistinctStream::new(
                left, right, schema, control.clone(),
                MAX_STREAM_DISTINCT_KEYS, MAX_STREAM_DISTINCT_BYTES,
            )?))
        }
        Plan::Distinct { input } => Ok(Box::new(DistinctOutput::new(
            child(input)?, control.clone(), MAX_STREAM_DISTINCT_KEYS, MAX_STREAM_DISTINCT_BYTES
        ))),
        Plan::CrossJoin { left, right } => {
            let left_schema = infer(left, catalog, depth + 1, options.max_plan_depth)?;
            let right_schema = infer(right, catalog, depth + 1, options.max_plan_depth)?;
            Ok(Box::new(CrossJoinStream::new(
                child(left)?, child(right)?, left_schema, right_schema,
                options.batch_rows, control.clone(),
            )?))
        }
        Plan::Join { left, right, left_key, right_key, kind } => {
            // Partitioned grace_join already tracks matched build keys across
            // all probe batches. Do not downgrade RIGHT/FULL to an inner join.
            let left_schema = infer(left, catalog, depth + 1, options.max_plan_depth)?;
            let right_schema = infer(right, catalog, depth + 1, options.max_plan_depth)?;
            let files = grace_join(
                child(left)?, child(right)?, left_schema, right_schema,
                left_key, right_key, *kind, &options.scratch_directory,
                16, 200_000, options.batch_rows, control
            )?;
            Ok(Box::new(JoinedOutput {
                files, reader: None, next_file: 0,
                batch_rows: options.batch_rows, control: control.clone(), stopped: false,
            }))
        }
        Plan::Aggregate { input, group_by, aggregates } => {
            let schema = infer(input, catalog, depth + 1, options.max_plan_depth)?;
            if group_by.is_empty() {
                let result = aggregate::aggregate_global_stream(
                    child(input)?, &schema, aggregates, || control.check(),
                )?;
                return Ok(Box::new(std::iter::once(Ok(result))));
            }
            let mut groups = Vec::with_capacity(group_by.len());
            for (expr, alias) in group_by {
                let Expr::Column(name) = expr else {
                    return Err(Error::Unsupported("spilled GROUP BY requires columns".into()));
                };
                groups.push((name.clone(), alias.clone()));
            }
            let files = grouped_aggregate(
                child(input)?, schema, &groups, aggregates,
                &options.scratch_directory, 16, 200_000, options.batch_rows, control
            )?;
            Ok(Box::new(GroupedOutput {
                files, reader: None, next_file: 0, batch_rows: options.batch_rows,
                control: control.clone(), stopped: false,
            }))
        }
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
        Plan::Intersect { left, right } |
        Plan::IntersectAll { left, right } |
        Plan::Except { left, right } |
        Plan::ExceptAll { left, right } => {
            let left_schema = infer(left, catalog, depth + 1, options.max_plan_depth)?;
            let right_schema = infer(right, catalog, depth + 1, options.max_plan_depth)?;
            let mode = match plan {
                Plan::Intersect { .. } => SetMode::IntersectDistinct,
                Plan::IntersectAll { .. } => SetMode::IntersectAll,
                Plan::Except { .. } => SetMode::ExceptDistinct,
                _ => SetMode::ExceptAll,
            };
            Ok(Box::new(SetStream::new(
                child(left)?, child(right)?, left_schema, right_schema, mode,
                control.clone(), MAX_STREAM_DISTINCT_KEYS, MAX_STREAM_DISTINCT_BYTES,
            )?))
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
    if options.batch_rows == 0 || options.batch_rows > 65_536 ||
        options.sort_run_rows == 0 ||
        options.sort_run_rows > 65_536 ||
        !(2..=64).contains(&options.merge_fan_in) || options.max_plan_depth == 0 {
        return Err(Error::Execution("invalid streaming executor settings".into()));
    }
    control.check()?;
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
    fn rejects_oversized_stream_output_batches_before_opening_files() {
        let options = StreamOptions { batch_rows: 65_537, ..StreamOptions::default() };
        let result = execute(&Plan::scan("unused"), &FileCatalog::new(),
            &options, QueryControl::default());
        assert!(matches!(result, Err(Error::Execution(_))));
        let options = StreamOptions { batch_rows: 65_536, ..StreamOptions::default() };
        assert!(matches!(execute(&Plan::scan("unused"), &FileCatalog::new(),
            &options, QueryControl::default()), Err(Error::Schema(_))));
    }

    #[test]
    fn rejects_invalid_stream_order_keys_before_reading_sources() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("t", "unused.csv", schema, 64).unwrap();
        let bad = Plan::Sort {
            input: Box::new(Plan::scan("t")),
            keys: vec![crate::sort::SortKey::ascending(
                crate::expr::Expr::Column("missing".into())
            )],
        };
        assert!(output_schema(&bad, &catalog, &StreamOptions::default()).is_err());
    }

    #[test]
    fn rejects_invalid_stream_filters_before_opening_sources() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("t", "unused.csv", schema, 64).unwrap();
        let bad = Plan::scan("t").filter(crate::expr::Expr::Column("id".into()));
        assert!(output_schema(&bad, &catalog, &StreamOptions::default()).is_err());
        let missing = Plan::scan("t").filter(crate::expr::Expr::Column("missing".into()));
        assert!(output_schema(&missing, &catalog, &StreamOptions::default()).is_err());
    }

    #[test]
    fn cancelled_streams_fail_before_inference_or_catalog_lookup() {
        let control = QueryControl::default();
        control.cancel();
        let result = execute(
            &Plan::scan("unknown"), &FileCatalog::new(),
            &StreamOptions::default(), control,
        );
        assert!(matches!(result, Err(Error::Execution(message)) if message.contains("cancelled")));
    }

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
mod union_inference_engine_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn union_matches_columns_by_position_and_widens_nullability() {
        let mut catalog = FileCatalog::new();
        catalog.register_csv("a", "a.csv", Schema::new(vec![
            Field::new("id", DataType::Integer, false),
        ]).unwrap(), 128).unwrap();
        catalog.register_csv("b", "b.csv", Schema::new(vec![
            Field::new("other", DataType::Integer, true),
        ]).unwrap(), 128).unwrap();
        let plan = Plan::UnionAll {
            left: Box::new(Plan::scan("a")), right: Box::new(Plan::scan("b"))
        };
        let inferred = output_schema(&plan, &catalog, &StreamOptions::default()).unwrap();
        assert_eq!(inferred.fields(), &[Field::new("id", DataType::Integer, true)]);
    }
}

#[cfg(test)]
mod consolidated_external_sort_preflight_tests {
    use super::*;
    use crate::{expr::Expr, sort::SortKey, schema::{DataType, Field}};
    #[test]
    fn unsupported_sort_expressions_fail_before_opening_source_files() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("t", "missing.csv", schema, 64).unwrap();
        let plan = Plan::Sort { input: Box::new(Plan::scan("t")),
            keys: vec![SortKey::ascending(Expr::Literal(Value::Integer(1)))] };
        let result = execute(&plan, &catalog, &StreamOptions::default(), QueryControl::default());
        assert!(matches!(result, Err(Error::Unsupported(_))));
    }
}

#[cfg(test)]
mod native_streamed_group_by_tests {
    use super::*;
    use crate::{aggregate::{AggregateItem, AggregateOp}, expr::Expr,
        schema::{DataType, Field}, scalar::Value};
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn sql_grouping_spills_and_merges_across_csv_batches() {
        let dir = std::env::temp_dir().join(format!("rustspark-stream-group-{}-{}",
            std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir_all(&dir).unwrap();
        let csv = dir.join("data.csv");
        fs::write(&csv, "g,n\n1,3\n2,5\n1,7\n2,\n").unwrap();
        let schema = Schema::new(vec![
            Field::new("g", DataType::Integer, false),
            Field::new("n", DataType::Integer, true),
        ]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("data", &csv, schema, 1024).unwrap();
        let plan = Plan::Aggregate {
            input: Box::new(Plan::scan("data")),
            group_by: vec![(Expr::Column("g".into()), "g".into())],
            aggregates: vec![
                AggregateItem::new(AggregateOp::CountAll, "rows"),
                AggregateItem::new(AggregateOp::Sum(Expr::Column("n".into())), "sum"),
            ],
        };
        let options = StreamOptions {
            batch_rows: 1, scratch_directory: dir.clone(), ..StreamOptions::default()
        };
        let rows = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![Value::Integer(1), Value::Integer(2), Value::Integer(10)]));
        assert!(rows.contains(&vec![Value::Integer(2), Value::Integer(2), Value::Integer(5)]));
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod native_streamed_join_tests {
    use super::*;
    use crate::{join::JoinKind, scalar::Value, schema::{DataType, Field}};
    use std::{fs, sync::atomic::{AtomicU64, Ordering}};
    static ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn partitioned_left_join_emits_matches_and_unmatched_once() {
        let dir = std::env::temp_dir().join(format!("rustspark-stream-join-{}-{}",
            std::process::id(), ID.fetch_add(1, Ordering::Relaxed)));
        fs::create_dir_all(&dir).unwrap();
        let left = dir.join("left.csv");
        let right = dir.join("right.csv");
        fs::write(&left, "id\n1\n2\n3\n").unwrap();
        fs::write(&right, "id\n2\n3\n").unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("lhs", &left, schema.clone(), 1024).unwrap();
        catalog.register_csv("rhs", &right, schema, 1024).unwrap();
        let plan = Plan::Join {
            left: Box::new(Plan::scan("lhs")),
            right: Box::new(Plan::scan("rhs")),
            left_key: "id".into(), right_key: "id".into(), kind: JoinKind::Left,
        };
        let options = StreamOptions { batch_rows: 1, scratch_directory: dir.clone(),
            ..StreamOptions::default() };
        let rows = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap().iter()
            .flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&vec![Value::Integer(1), Value::Null]));
        assert!(rows.contains(&vec![Value::Integer(2), Value::Integer(2)]));
        assert!(rows.contains(&vec![Value::Integer(3), Value::Integer(3)]));
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod stream_distinct_single_probe_perf_tests {
    use super::*;

    #[test]
    fn repeated_wide_rows_remain_valid_at_key_limit() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, false)]).unwrap();
        let first = Value::Text("a".repeat(512));
        let second = Value::Text("b".repeat(512));
        let data = Batch::from_rows(schema.clone(), vec![
            vec![first.clone()], vec![second.clone()], vec![first.clone()], vec![second.clone()]
        ]).unwrap();
        let source: BatchStream = Box::new([Ok(data)].into_iter());
        let results = DistinctOutput::new(source, QueryControl::default(), 2, 4096)
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.into_iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>(),
            vec![vec![first], vec![second]]);
        let overflowing = Batch::from_rows(schema, vec![
            vec![Value::Text("a".into())], vec![Value::Text("b".into())]
        ]).unwrap();
        let source: BatchStream = Box::new([Ok(overflowing)].into_iter());
        assert!(DistinctOutput::new(source, QueryControl::default(), 1, 4096)
            .next().unwrap().is_err());
    }
}

#[cfg(test)]
mod distinct_stream_end_of_input_tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[test]
    fn does_not_poll_upstream_after_first_end_of_stream() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polls);
        let source = std::iter::from_fn(move || {
            let call = observed.fetch_add(1, Ordering::Relaxed);
            if call == 0 { None } else { panic!("distinct polled exhausted source") }
        });
        let mut distinct = DistinctOutput::new(Box::new(source),
            QueryControl::default(), 100, 4096);
        assert!(distinct.next().is_none());
        assert!(distinct.next().is_none());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod streaming_global_aggregate_sql_tests {
    use super::*;
    use crate::{sql, scalar::Value, schema::{DataType, Field}};
    use std::fs;

    #[test]
    fn sql_count_sum_and_avg_across_file_batches() {
        let path = std::env::temp_dir().join(format!("native-global-agg-{}-{}.csv",
            std::process::id(), std::time::SystemTime::now().duration_since(
                std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::write(&path, "n\n1\n2\n3\n4\n").unwrap();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("t", &path, schema, 128).unwrap();
        let plan = sql::parse_sql(
            "SELECT COUNT(*) AS n, SUM(n) AS total, AVG(n) AS average FROM t").unwrap();
        let options = StreamOptions { batch_rows: 2, ..StreamOptions::default() };
        let rows = execute(&plan, &catalog, &options, QueryControl::default())
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row(0).unwrap(), vec![
            Value::Integer(4), Value::Integer(10), Value::Float(2.5),
        ]);
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod direct_union_distinct_lowering_tests {
    use super::*;
    use crate::{schema::{DataType, Field}, scalar::Value, sql};
    use std::fs;

    #[test]
    fn union_distinct_keeps_first_rows_and_nulls_across_csv_batches() {
        let dir = std::env::temp_dir().join(format!("native-union-distinct-{}-{}",
            std::process::id(), std::time::SystemTime::now().duration_since(
                std::time::UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir(&dir).unwrap();
        let left = dir.join("a.csv");
        let right = dir.join("b.csv");
        fs::write(&left, "id\n1\n2\n\n").unwrap();
        fs::write(&right, "id\n2\n3\n\n").unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let mut catalog = FileCatalog::new();
        catalog.register_csv("a", &left, schema.clone(), 256).unwrap();
        catalog.register_csv("b", &right, schema, 256).unwrap();
        let plan = sql::parse_sql("SELECT id FROM a UNION SELECT id FROM b").unwrap();
        let parts = execute(&plan, &catalog, &StreamOptions {
            batch_rows: 1, scratch_directory: dir.clone(), ..StreamOptions::default()
        }, QueryControl::default()).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), vec![
                vec![Value::Integer(1)], vec![Value::Integer(2)],
                vec![Value::Integer(3)],
            ]);
        fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod zero_width_stream_distinct_tests {
    use super::*;
    #[test]
    fn returns_one_row_without_hashing_large_empty_keys() {
        let schema = Schema::new(vec![]).unwrap();
        let first = Batch::new_with_row_count(schema.clone(), vec![], 100_000).unwrap();
        let second = Batch::new_with_row_count(schema.clone(), vec![], 200_000).unwrap();
        let output = DistinctOutput::new(
            Box::new([Ok(first), Ok(second)].into_iter()), QueryControl::default(), 1, 64)
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).sum::<usize>(), 1);
        let input = Batch::new_with_row_count(schema, vec![], 2).unwrap();
        let mut limited = DistinctOutput::new(
            Box::new([Ok(input)].into_iter()), QueryControl::default(), 1, 63);
        assert!(limited.next().unwrap().is_err());
        assert!(limited.next().is_none());
    }
}
