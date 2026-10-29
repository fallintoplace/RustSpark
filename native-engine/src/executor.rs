use crate::{
    aggregate, catalog::Catalog, distinct, filter, join, limit,
    plan::Plan, project, sort, union, set_ops,
    cancel::QueryGuard,
    batch::Batch, Error, Result,
};

#[derive(Clone, Copy, Debug)]
pub struct ExecutionOptions {
    /// Protect this experimental in-memory engine against runaway results.
    pub max_output_rows: usize,
    pub max_plan_depth: usize,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            max_output_rows: 1_000_000,
            max_plan_depth: 64,
        }
    }
}

pub fn execute(plan: &Plan, catalog: &Catalog) -> Result<Batch> {
    execute_with_options(plan, catalog, ExecutionOptions::default())
}

pub fn execute_with_options(
    plan: &Plan,
    catalog: &Catalog,
    options: ExecutionOptions,
) -> Result<Batch> {
    execute_with_guard(plan, catalog, options, &QueryGuard::default())
}

/// Check cancellation before and after each plan node.
/// Blocking operators will support finer-grained interruption separately.
pub fn execute_with_guard(
    plan: &Plan,
    catalog: &Catalog,
    options: ExecutionOptions,
    guard: &QueryGuard,
) -> Result<Batch> {
    let optimized = crate::optimizer::optimize(plan.clone());
    execute_inner(&optimized, catalog, &options, 0, guard)
}

fn execute_inner(
    plan: &Plan,
    catalog: &Catalog,
    options: &ExecutionOptions,
    depth: usize,
    guard: &QueryGuard,
) -> Result<Batch> {
    if depth >= options.max_plan_depth {
        return Err(Error::Execution("query plan exceeds maximum depth".into()));
    }
    guard.check()?;
    let descend = |child: &Plan| execute_inner(child, catalog, options, depth + 1, guard);
    let batch = match plan {
        Plan::Scan { table } => catalog.get(table)?.clone(),
        Plan::Filter { input, predicate } => filter::filter(&descend(input)?, predicate)?,
        Plan::Project { input, selections } => project::project(&descend(input)?, selections)?,
        Plan::Aggregate { input, group_by, aggregates } => {
            aggregate::aggregate(&descend(input)?, group_by, aggregates)?
        }
        Plan::Sort { input, keys } => sort::sort(&descend(input)?, keys)?,
        Plan::Limit { input, offset, count } => limit::limit(&descend(input)?, *offset, *count)?,
        Plan::Distinct { input } => distinct::distinct(&descend(input)?)?,
        Plan::Join {
            left,
            right,
            left_key,
            right_key,
            kind,
        } => join::hash_join(
            &descend(left)?,
            &descend(right)?,
            left_key,
            right_key,
            *kind,
        )?,
        Plan::UnionAll { left, right } => union::union_all(&descend(left)?, &descend(right)?)?,
        Plan::Intersect { left, right } => set_ops::intersect_distinct(&descend(left)?, &descend(right)?)?,
        Plan::Except { left, right } => set_ops::except_distinct(&descend(left)?, &descend(right)?)?,
    };
    guard.check()?;
    if batch.num_rows() > options.max_output_rows {
        return Err(Error::Execution(format!(
            "query output exceeds {} rows",
            options.max_output_rows
        )));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::{BinaryOp, Expr},
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn cancelled_queries_stop_before_reading_sources() {
        use crate::cancel::Cancellation;
        let token = Cancellation::new();
        token.cancel();
        let guard = QueryGuard::new(token);
        let error = execute_with_guard(
            &Plan::scan("unregistered"),
            &Catalog::new(),
            ExecutionOptions::default(),
            &guard,
        ).unwrap_err();
        assert!(error.to_string().contains("cancelled"));
    }

    #[test]
    fn query_deadlines_stop_before_execution() {
        use crate::cancel::{Cancellation, QueryGuard};
        use std::time::Duration;
        let guard = QueryGuard::with_timeout(
            Cancellation::new(), Duration::ZERO,
        ).unwrap();
        let error = execute_with_guard(
            &Plan::scan("unused"),
            &Catalog::new(),
            ExecutionOptions::default(),
            &guard,
        ).unwrap_err();
        assert!(error.to_string().contains("deadline"));
    }

    #[test]
    fn scan_filter_limit_runs_without_datafusion() {
        let mut catalog = Catalog::new();
        let data = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
                vec![Value::Integer(3)],
            ],
        )
        .unwrap();
        catalog.register("data", data).unwrap();
        let plan = Plan::scan("data")
            .filter(Expr::binary(
                Expr::Column("x".into()),
                BinaryOp::Greater,
                Expr::Literal(Value::Integer(1)),
            ))
            .limit(0, Some(1));
        assert_eq!(execute(&plan, &catalog).unwrap().rows().unwrap(), vec![vec![Value::Integer(2)]]);
    }

    #[test]
    fn depth_and_result_limits_reject_runaway_queries() {
        let mut catalog = Catalog::new();
        catalog.register("x", Batch::empty(Schema::new(vec![]).unwrap())).unwrap();
        assert!(execute_with_options(
            &Plan::scan("x"),
            &catalog,
            ExecutionOptions { max_output_rows: 5, max_plan_depth: 0 }
        ).is_err());
    }
}
