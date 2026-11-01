//! Conservative rewrites of native logical plans.
//! These rules never reorder expressions across filters or joins.

use crate::{
    expr::Expr,
    plan::Plan,
    scalar::Value,
};

// Calls and complex expressions may be volatile. Only fold predicates whose
// evaluation cannot change between adjacent filter operators.
fn repeatable_filter(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_) => true,
        Expr::Binary { left, right, .. } =>
            repeatable_filter(left) && repeatable_filter(right),
        Expr::Not(inner) | Expr::IsNull(inner) => repeatable_filter(inner),
        _ => false,
    }
}

pub fn optimize(plan: Plan) -> Plan {
    match plan {
        Plan::OneRow | Plan::Scan { .. } | Plan::Values { .. } => plan,
        Plan::Filter { input, predicate } => {
            let input = optimize(*input);
            if matches!(&predicate, Expr::Literal(Value::Boolean(true))) {
                input
            } else if matches!(&input, Plan::Filter { predicate: inner, .. }
                if inner == &predicate && repeatable_filter(&predicate)) {
                // The inner predicate already admitted exactly these rows.
                input
            } else if let Plan::Filter { input: grandchild, predicate: inner } = input {
                if repeatable_filter(&inner) && repeatable_filter(&predicate) {
                    // Two filters skip the outer predicate when the inner
                    // predicate is FALSE or NULL. Bare AND would still
                    // evaluate the outer expression for NULL.
                    Plan::Filter {
                        input: grandchild,
                        predicate: Expr::Case {
                            branches: vec![(inner, predicate)],
                            else_expr: Some(Box::new(Expr::Literal(Value::Boolean(false)))),
                        },
                    }
                } else {
                    Plan::Filter {
                        input: Box::new(Plan::Filter { input: grandchild, predicate: inner }),
                        predicate,
                    }
                }
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
                matches!(&input, Plan::Sort { keys: inner_keys, .. }
                    if inner_keys == &keys &&
                        keys.iter().all(|key| repeatable_filter(&key.expression))) {
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
                        // Both input windows are valid, but composing their
                        // offsets can make the merged window overflow.
                        // Preserve the original operators in that case.
                        if merged_count.is_none_or(|limit| merged_offset.checked_add(limit).is_some()) {
                            return Plan::Limit {
                                input: grandchild,
                                offset: merged_offset,
                                count: merged_count,
                            };
                        }
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
    fn nested_limit_merge_must_not_introduce_window_overflow() {
        // Each LIMIT is valid on its own, and the inner one produces
        // no rows. Combining them produces MAX + 1, which is invalid.
        let plan = Plan::scan("sales")
            .limit(usize::MAX - 1, None)
            .limit(1, Some(1));
        assert_eq!(optimize(plan.clone()), plan);

        let mut catalog = Catalog::new();
        catalog.register("sales", Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap()).unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap().num_rows(), 0);
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
mod repeated_pure_filter_regressions {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor,
        expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn folds_adjacent_repeatable_filters_without_changing_results() {
        let predicate = Expr::binary(Expr::Column("n".into()),
            BinaryOp::Greater, Expr::Literal(Value::Integer(3)));
        let plan = Plan::scan("t").filter(predicate.clone()).filter(predicate.clone());
        let optimized = optimize(plan.clone());
        assert_eq!(optimized, Plan::scan("t").filter(predicate));
        let mut catalog = Catalog::new();
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Integer, true)
        ]).unwrap(), (0..10).map(|n| vec![Value::Integer(n)]).chain(
            [vec![Value::Null]]).collect()).unwrap();
        catalog.register("t", batch).unwrap();
        assert_eq!(executor::execute(&plan, &catalog).unwrap(),
            executor::execute(&optimized, &catalog).unwrap());
    }

    #[test]
    fn leaves_function_calls_and_intervening_projections_intact() {
        let uncertain = Expr::Call {
            name: "isnull".into(), args: vec![Expr::Column("n".into())]
        };
        let nested = Plan::scan("t").filter(uncertain.clone()).filter(uncertain);
        assert_eq!(optimize(nested.clone()), nested);
    }
}

#[cfg(test)]
mod volatile_sort_optimizer_regressions {
    use super::*;
    use crate::sort::SortKey;

    #[test]
    fn duplicate_function_sort_must_remain_in_execution_plan() {
        let uncertain = Expr::Call {
            name: "random".into(), args: vec![],
        };
        for expression in [
            uncertain.clone(),
            Expr::binary(Expr::Literal(Value::Integer(1)),
                crate::expr::BinaryOp::Add, uncertain),
        ] {
            let keys = vec![SortKey::ascending(expression)];
            let once = Plan::Sort {
                input: Box::new(Plan::scan("t")), keys: keys.clone(),
            };
            let twice = Plan::Sort {
                input: Box::new(once), keys,
            };
            assert_eq!(optimize(twice.clone()), twice);
        }
    }

    #[test]
    fn deterministic_sort_keys_still_deduplicate() {
        let keys = vec![SortKey::ascending(Expr::Column("v".into()))];
        let once = Plan::Sort {
            input: Box::new(Plan::scan("t")), keys: keys.clone(),
        };
        let twice = Plan::Sort { input: Box::new(once.clone()), keys };
        assert_eq!(optimize(twice), once);
    }
}

#[cfg(test)]
mod fused_filter_plan_tests {
    use super::*;
    use crate::{batch::Batch, catalog::Catalog, executor, expr::BinaryOp,
        schema::{DataType, Field, Schema}};

    #[test]
    fn adjacent_pure_filters_share_one_scan_and_preserve_nulls() {
        let low = Expr::binary(Expr::Column("x".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(2)));
        let high = Expr::binary(Expr::Column("x".into()), BinaryOp::Less,
            Expr::Literal(Value::Integer(8)));
        let source = Plan::scan("t");
        let original = source.clone().filter(low.clone()).filter(high.clone());
        let optimized = optimize(original.clone());
        assert_eq!(optimized, source.filter(Expr::Case {
            branches: vec![(low, high)],
            else_expr: Some(Box::new(Expr::Literal(Value::Boolean(false)))),
        }));
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap();
        let rows = (0..11).map(|x| vec![Value::Integer(x)])
            .chain([vec![Value::Null]]).collect();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, rows).unwrap()).unwrap();
        assert_eq!(executor::execute(&original, &catalog).unwrap(),
            executor::execute(&optimized, &catalog).unwrap());
    }

    #[test]
    fn null_or_false_inner_predicate_skips_erroring_outer_expression() {
        let failing = Expr::binary(
            Expr::binary(Expr::Literal(Value::Integer(1)), BinaryOp::Divide,
                Expr::Literal(Value::Integer(0))),
            BinaryOp::Greater, Expr::Literal(Value::Integer(0)));
        let original = Plan::scan("t")
            .filter(Expr::Column("active".into()))
            .filter(failing);
        let optimized = optimize(original.clone());
        assert_ne!(optimized, original);
        let schema = Schema::new(vec![Field::new("active", DataType::Boolean, true)]).unwrap();
        let mut catalog = Catalog::new();
        catalog.register("t", Batch::from_rows(schema, vec![
            vec![Value::Null], vec![Value::Boolean(false)],
        ]).unwrap()).unwrap();
        assert_eq!(executor::execute(&optimized, &catalog).unwrap(),
            executor::execute(&original, &catalog).unwrap());
    }

    #[test]
    fn do_not_fuse_uncertain_function_expressions() {
        let pure = Expr::Column("is_active".into());
        let uncertain = Expr::Call { name: "random".into(), args: vec![] };
        let plan = Plan::scan("t").filter(pure).filter(uncertain);
        assert_eq!(optimize(plan.clone()), plan);
    }
}
