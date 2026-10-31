use crate::{
    aggregate, catalog::Catalog, distinct, filter, join, limit,
    plan::Plan, project, sort, union, set_ops,
    cancel::QueryGuard,
    batch::Batch, schema::Schema, Error, Result,
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

fn validate_depth(plan: &Plan, max_depth: usize) -> Result<()> {
    const MAX_NODES: usize = 4096;
    let mut pending = vec![(plan, 0_usize)];
    let mut visited = 0usize;
    while let Some((node, depth)) = pending.pop() {
        visited += 1;
        if visited > MAX_NODES {
            return Err(Error::Execution("query plan exceeds maximum node count".into()));
        }
        if depth >= max_depth {
            return Err(Error::Execution("query plan exceeds maximum depth".into()));
        }
        match node {
            Plan::OneRow | Plan::Scan { .. } | Plan::Values { .. } => {}
            Plan::Filter { input, .. }
            | Plan::Project { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::Distinct { input } => pending.push((input, depth + 1)),
            Plan::Join { left, right, .. }
            | Plan::CrossJoin { left, right }
            | Plan::UnionAll { left, right }
            | Plan::Intersect { left, right }
            | Plan::IntersectAll { left, right }
            | Plan::ExceptAll { left, right }
            | Plan::Except { left, right } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
        }
    }
    Ok(())
}

/// Check cancellation before and after each plan node.
/// Blocking operators will support finer-grained interruption separately.
pub fn execute_with_guard(
    plan: &Plan,
    catalog: &Catalog,
    options: ExecutionOptions,
    guard: &QueryGuard,
) -> Result<Batch> {
    guard.check()?;
    validate_depth(plan, options.max_plan_depth)?;
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
        Plan::Values { batch } => batch.clone(),
        Plan::OneRow => Batch::new_with_row_count(Schema::new(vec![])?, vec![], 1)?,
        Plan::Scan { table } => catalog.get(table)?.clone(),
        Plan::Filter { input, predicate } => {
            if let Plan::Scan { table } = input.as_ref() {
                // Avoid cloning the full source table before selecting rows.
                filter::filter(catalog.get(table)?, predicate)?
            } else {
                filter::filter(&descend(input)?, predicate)?
            }
        },
        Plan::Project { input, selections } => {
            if let Plan::Scan { table } = input.as_ref() {
                project::project(catalog.get(table)?, selections)?
            } else {
                project::project_owned(descend(input)?, selections)?
            }
        },
        Plan::Aggregate { input, group_by, aggregates } => {
            if let Plan::Scan { table } = input.as_ref() {
                aggregate::aggregate(catalog.get(table)?, group_by, aggregates)?
            } else {
                aggregate::aggregate(&descend(input)?, group_by, aggregates)?
            }
        }
        Plan::Sort { input, keys } => sort::sort(&descend(input)?, keys)?,
        Plan::Limit { input, offset, count: Some(count) } if *count > 0
            && matches!(input.as_ref(), Plan::Sort { .. }) => {
            let Plan::Sort { input: source, keys } = input.as_ref() else {
                unreachable!("checked sort plan");
            };
            let source = execute_inner(source, catalog, options, depth + 2, guard)?;
            guard.check()?;
            crate::topk::top_k_offset(&source, keys, *offset, *count)?
        },
        Plan::Limit { input, offset, count } => {
            // A LIMIT over a scan must not materialize and budget the whole input.
            if let Plan::Scan { table } = input.as_ref() {
                limit::limit(catalog.get(table)?, *offset, *count)?
            } else {
                limit::limit(&descend(input)?, *offset, *count)?
            }
        },
        Plan::Distinct { input } => {
            if let Plan::Scan { table } = input.as_ref() {
                distinct::distinct(catalog.get(table)?)?
            } else {
                distinct::distinct(&descend(input)?)?
            }
        },
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
        Plan::CrossJoin { left, right } => join::cross_join_with_max_rows(
            &descend(left)?, &descend(right)?, options.max_output_rows
        )?,
        Plan::UnionAll { left, right } => union::union_all(&descend(left)?, &descend(right)?)?,
        Plan::Intersect { left, right } => set_ops::intersect_distinct(&descend(left)?, &descend(right)?)?,
        Plan::IntersectAll { left, right } => set_ops::intersect_all(&descend(left)?, &descend(right)?)?,
        Plan::ExceptAll { left, right } => set_ops::except_all(&descend(left)?, &descend(right)?)?,
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
    fn filtered_scan_does_not_materialize_oversized_source() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        catalog.register("large", Batch::from_rows(schema,
            (0..20).map(|n| vec![Value::Integer(n)]).collect()).unwrap()).unwrap();
        let predicate = Expr::binary(
            Expr::Column("n".into()), BinaryOp::GreaterEqual,
            Expr::Literal(Value::Integer(19)),
        );
        let options = ExecutionOptions { max_output_rows: 1, ..ExecutionOptions::default() };
        let output = execute_with_options(&Plan::scan("large").filter(predicate),
            &catalog, options).unwrap();
        assert_eq!(output.rows().unwrap(), vec![vec![Value::Integer(19)]]);
        assert!(execute_with_options(&Plan::scan("large"), &catalog, options).is_err());
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
    #[test]
    fn direct_scan_limit_respects_final_result_budget() {
        let mut catalog = Catalog::new();
        let data = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..20).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap();
        catalog.register("large", data).unwrap();
        let options = ExecutionOptions { max_output_rows: 2, ..ExecutionOptions::default() };
        let rows = execute_with_options(&Plan::scan("large").limit(11, Some(2)), &catalog, options)
            .unwrap().rows().unwrap();
        assert_eq!(rows, vec![vec![Value::Integer(11)], vec![Value::Integer(12)]]);
        assert!(execute_with_options(&Plan::scan("large"), &catalog, options).is_err());
    }
}

#[cfg(test)]
mod plan_depth_preflight_tests {
    use super::*;

    #[test]
    fn rejects_deep_plans_before_optimization_or_scanning() {
        let mut plan = Plan::scan("missing");
        for _ in 0..256 {
            plan = plan.limit(0, Some(1));
        }
        let result = execute_with_options(
            &plan, &Catalog::new(),
            ExecutionOptions { max_output_rows: 100, max_plan_depth: 32 },
        );
        assert!(result.unwrap_err().to_string().contains("maximum depth"));
    }
}

#[cfg(test)]
mod consolidated_sort_limit_fastpath_tests {
    use super::*;
    use crate::{expr::Expr, scalar::Value, schema::{DataType, Field, Schema}, sort::SortKey};
    #[test]
    fn order_by_limit_keeps_stable_ties_and_offset() {
        let mut catalog = Catalog::new();
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        catalog.register("t", Batch::from_rows(schema,
            vec![vec![Value::Integer(3)], vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap()).unwrap();
        let plan = Plan::Sort { input: Box::new(Plan::scan("t")),
            keys: vec![SortKey::ascending(Expr::Column("n".into()))] }.limit(1, Some(2));
        assert_eq!(execute(&plan, &catalog).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    }
}

#[cfg(test)]
mod logical_plan_width_budget_tests {
    use super::*;

    fn balanced_plan(depth: usize) -> Plan {
        if depth == 0 { return Plan::scan("source"); }
        Plan::UnionAll {
            left: Box::new(balanced_plan(depth - 1)),
            right: Box::new(balanced_plan(depth - 1)),
        }
    }

    #[test]
    fn rejects_wide_logical_trees_before_optimizer_allocates() {
        let oversized = balanced_plan(12);
        let error = validate_depth(&oversized, 64).unwrap_err();
        assert!(error.to_string().contains("node count"));
        assert!(validate_depth(&balanced_plan(8), 64).is_ok());
    }
}

#[cfg(test)]
mod catalog_projection_borrow_tests {
    use super::*;
    use crate::{expr::Expr, project::Selection, schema::{DataType, Field, Schema}, scalar::Value};
    #[test]
    fn projects_directly_from_catalog_with_nullable_values_and_aliases() {
        let mut catalog = Catalog::new();
        catalog.register("source", Batch::from_rows(
            Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("hello".into())], vec![Value::Null]]
        ).unwrap()).unwrap();
        let plan = Plan::Project {
            input: Box::new(Plan::scan("source")),
            selections: vec![Selection::new(Expr::Column("text".into()), "alias")],
        };
        let result = execute(&plan, &catalog).unwrap();
        assert_eq!(result.rows().unwrap(),
            vec![vec![Value::Text("hello".into())], vec![Value::Null]]);
        assert_eq!(result.schema().field(0).unwrap().name, "alias");
    }
}

#[cfg(test)]
mod catalog_aggregate_borrow_tests {
    use super::*;
    use crate::{aggregate::{AggregateItem, AggregateOp},
        schema::{DataType, Field, Schema}, scalar::Value};
    #[test]
    fn counts_a_large_catalog_with_a_single_row_output_budget() {
        let mut catalog = Catalog::new();
        catalog.register("source", Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..100).map(|n| vec![Value::Integer(n)]).collect()
        ).unwrap()).unwrap();
        let plan = Plan::Aggregate {
            input: Box::new(Plan::scan("source")),
            group_by: vec![],
            aggregates: vec![AggregateItem::new(AggregateOp::CountAll, "rows")],
        };
        let result = execute_with_options(&plan, &catalog,
            ExecutionOptions { max_output_rows: 1, ..ExecutionOptions::default() }).unwrap();
        assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(100)]]);
    }
}

#[cfg(test)]
mod catalog_distinct_borrow_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};
    #[test]
    fn deduplicates_large_catalog_scan_to_small_budget() {
        let mut catalog = Catalog::new();
        catalog.register("source", Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            (0..100).map(|n| vec![if n % 2 == 0 { Value::Null }
                else { Value::Integer(7) }]).collect()
        ).unwrap()).unwrap();
        let result = execute_with_options(&Plan::Distinct {
                input: Box::new(Plan::scan("source")),
            }, &catalog, ExecutionOptions { max_output_rows: 2,
                ..ExecutionOptions::default() }).unwrap();
        assert_eq!(result.rows().unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(7)]]);
    }
}

#[cfg(test)]
mod owned_intermediate_project_execution_tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, project::Selection, scalar::Value,
        schema::{DataType, Field, Schema}};

    #[test]
    fn projects_filtered_text_with_computation_and_duplicate_aliases() {
        let mut catalog = Catalog::new();
        let large = "東京🙂".repeat(2048);
        catalog.register("t", Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Text(large.clone())],
            vec![Value::Integer(2), Value::Null],
            vec![Value::Integer(3), Value::Text("small".into())],
        ]).unwrap()).unwrap();
        let predicate = Expr::binary(Expr::Column("id".into()), BinaryOp::LessEqual,
            Expr::Literal(Value::Integer(2)));
        let source = Plan::scan("t").filter(predicate);
        let selections = vec![
            Selection::new(Expr::Column("note".into()), "copy"),
            Selection::new(Expr::Column("note".into()), "moved"),
            Selection::new(Expr::binary(Expr::Column("id".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(9))), "id_plus"),
        ];
        let input = execute(&source, &catalog).unwrap();
        let expected = project::project(&input, &selections).unwrap();
        let actual = execute(&Plan::Project {
            input: Box::new(source), selections,
        }, &catalog).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.rows().unwrap()[0][2], Value::Integer(10));
        assert_eq!(actual.rows().unwrap()[1][0], Value::Null);
    }
}
