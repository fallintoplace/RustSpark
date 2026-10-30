//! Conservative rewrites of native logical plans.
//! These rules never reorder expressions across filters or joins.

use crate::{
    expr::Expr,
    plan::Plan,
    scalar::Value,
};

pub fn optimize(plan: Plan) -> Plan {
    match plan {
        Plan::Scan { .. } => plan,
        Plan::Filter { input, predicate } => {
            let input = optimize(*input);
            if matches!(&predicate, Expr::Literal(Value::Boolean(true))) {
                input
            } else {
                Plan::Filter { input: Box::new(input), predicate }
            }
        }
        Plan::Project { input, selections } => Plan::Project {
            input: Box::new(optimize(*input)),
            selections,
        },
        Plan::Aggregate { input, group_by, aggregates } => Plan::Aggregate {
            input: Box::new(optimize(*input)),
            group_by,
            aggregates,
        },
        Plan::Sort { input, keys } => {
            let input = optimize(*input);
            if keys.is_empty() ||
                matches!(&input, Plan::Sort { keys: inner_keys, .. } if inner_keys == &keys) {
                input
            } else {
                Plan::Sort { input: Box::new(input), keys }
            }
        }
        Plan::Limit { input, offset, count } => {
            let child = optimize(*input);
            if offset == 0 && count.is_none() {
                return child;
            }
            // Projection is row-preserving. Slice first so computed expressions
            // only run on visible rows, without crossing filters or joins.
            // Keep invalid LIMIT arithmetic for runtime validation.
            let child = if count.is_none_or(|limit| offset.checked_add(limit).is_some()) {
                match child {
                    Plan::Project { input, selections } => {
                        return Plan::Project {
                            input: Box::new(optimize(Plan::Limit { input, offset, count })),
                            selections,
                        };
                    }
                    child => child,
                }
            } else {
                child
            };
            if let Plan::Limit {
                input: grandchild,
                offset: inner_offset,
                count: inner_count,
            } = child
            {
                // Keep the original operator order when arithmetic is invalid.
                let inner_valid = inner_count.is_none_or(|limit| inner_offset.checked_add(limit).is_some());
                let outer_valid = count.is_none_or(|limit| offset.checked_add(limit).is_some());
                if inner_valid && outer_valid {
                    if let Some(merged_offset) = inner_offset.checked_add(offset) {
                        let available = inner_count.map(|limit| limit.saturating_sub(offset));
                        let merged_count = match (count, available) {
                            (Some(outer), Some(inner)) => Some(outer.min(inner)),
                            (Some(outer), None) => Some(outer),
                            (None, Some(inner)) => Some(inner),
                            (None, None) => None,
                        };
                        return Plan::Limit {
                            input: grandchild,
                            offset: merged_offset,
                            count: merged_count,
                        };
                    }
                }
                Plan::Limit {
                    input: Box::new(Plan::Limit {
                        input: grandchild,
                        offset: inner_offset,
                        count: inner_count,
                    }),
                    offset,
                    count,
                }
            } else {
                Plan::Limit { input: Box::new(child), offset, count }
            }
        }
        Plan::Distinct { input } => {
            let child = optimize(*input);
            if matches!(&child, Plan::Distinct { .. }) {
                child
            } else {
                Plan::Distinct { input: Box::new(child) }
            }
        }
        Plan::Join {
            left, right, left_key, right_key, kind,
        } => Plan::Join {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
            left_key,
            right_key,
            kind,
        },
        Plan::CrossJoin { left, right } => Plan::CrossJoin {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::UnionAll { left, right } => Plan::UnionAll {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::Intersect { left, right } => Plan::Intersect {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::IntersectAll { left, right } => Plan::IntersectAll {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::ExceptAll { left, right } => Plan::ExceptAll {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::Except { left, right } => Plan::Except {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::Catalog,
        batch::Batch,
        executor,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn merges_nested_limits_without_changing_results() {
        let plan = Plan::scan("sales").limit(1, Some(4)).limit(1, Some(2));
        let optimized = optimize(plan.clone());
        assert_eq!(optimized, Plan::scan("sales").limit(2, Some(2)));
        let mut catalog = Catalog::new();
        catalog.register("sales", Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            (0..6).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap()).unwrap();
        assert_eq!(
            executor::execute(&plan, &catalog).unwrap(),
            executor::execute(&optimized, &catalog).unwrap(),
        );
    }

    #[test]
    fn removes_true_filters_and_redundant_distinct() {
        let plan = Plan::Distinct {
            input: Box::new(Plan::Distinct {
                input: Box::new(Plan::Scan { table: "sales".into() }.filter(
                    Expr::Literal(Value::Boolean(true)),
                )),
            }),
        };
        assert_eq!(optimize(plan), Plan::Distinct {
            input: Box::new(Plan::scan("sales")),
        });
    }

    #[test]
    fn preserves_invalid_limit_arithmetic_for_runtime_validation() {
        let plan = Plan::scan("sales")
            .limit(usize::MAX, Some(1))
            .limit(0, Some(1));
        assert_eq!(optimize(plan.clone()), plan);
    }
    #[test]
    fn redundant_stable_sorts_are_removed() {
        let keys = vec![crate::sort::SortKey::ascending(Expr::Column("x".into()))];
        let once = Plan::Sort { input: Box::new(Plan::scan("sales")), keys: keys.clone() };
        let twice = Plan::Sort { input: Box::new(once.clone()), keys };
        assert_eq!(optimize(twice), once);
    }
}

#[cfg(test)]
mod native_identity_limit_optimizer_tests {
    use super::*;

    #[test]
    fn drops_only_identity_limits_and_preserves_nested_bounds() {
        let source = Plan::scan("t");
        assert_eq!(optimize(source.clone().limit(0, None)), source);
        assert_eq!(optimize(source.clone().limit(2, None)), source.clone().limit(2, None));
        assert_eq!(optimize(source.clone().limit(0, Some(0))), source.clone().limit(0, Some(0)));
        let nested = source.clone().limit(0, None).limit(1, Some(2));
        assert_eq!(optimize(nested), source.limit(1, Some(2)));
    }
}

#[cfg(test)]
mod projected_limit_pushdown_tests {
    use super::*;
    use crate::{
        executor::{execute_with_options, ExecutionOptions},
        expr::BinaryOp,
        project::Selection,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn project_limit_executes_only_selected_source_window() {
        let selection = Selection::new(Expr::binary(
            Expr::Column("n".into()), BinaryOp::Add,
            Expr::Literal(Value::Integer(1)),
        ), "n_plus_one");
        let plan = Plan::scan("t").project(vec![selection.clone()]).limit(3, Some(2));
        assert_eq!(optimize(plan.clone()),
            Plan::scan("t").limit(3, Some(2)).project(vec![selection]));
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, (0..100)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap()).unwrap();
        let output = execute_with_options(&plan, &catalog, ExecutionOptions {
            max_output_rows: 2,
            ..ExecutionOptions::default()
        }).unwrap();
        assert_eq!(output.rows().unwrap(), vec![
            vec![Value::Integer(4)], vec![Value::Integer(5)],
        ]);
    }

    #[test]
    fn does_not_push_a_limit_through_a_filter_or_aggregate() {
        let filter = Plan::scan("t").filter(Expr::binary(
            Expr::Column("n".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(0)),
        ))
            .project(vec![Selection::new(Expr::Column("n".into()), "n")])
            .limit(0, Some(1));
        // The projection can move, but LIMIT must remain after WHERE.
        let plan = optimize(filter);
        let Plan::Project { input, .. } = plan else { panic!("expected projection"); };
        let Plan::Limit { input, .. } = *input else { panic!("expected limit"); };
        assert!(matches!(*input, Plan::Filter { .. }));
    }
}
