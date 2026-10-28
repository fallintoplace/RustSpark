//! Streaming physical execution of the supported native logical-plan subset.
use std::{path::PathBuf};
use crate::{
    batch::Batch, plan::Plan, project::Selection,
    schema::{Field, Schema}, Error, Result,
};
use super::{
    cancel::QueryControl,
    chunks::RowBatcher, file_catalog::FileCatalog,
    stream_ops::{FilterStream, ProjectStream, LimitStream},
    union_stream::UnionAllStream,
};

pub type BatchStream = Box<dyn Iterator<Item = Result<Batch>> + Send>;

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
            sort_run_rows: 65_536, merge_fan_in: 16,
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
        Plan::Aggregate { .. } | Plan::Join { .. } =>
            Err(Error::Unsupported("streaming schema inference is unavailable for this operator".into())),
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
        Plan::Project { input, selections } =>
            Ok(Box::new(ProjectStream::new(child(input)?, selections.clone(), control.clone()))),
        Plan::Limit { input, offset, count } =>
            Ok(Box::new(LimitStream::new(child(input)?, *offset, *count, control.clone()))),
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
