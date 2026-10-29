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
            if keys.is_empty() {
                input
            } else {
                Plan::Sort { input: Box::new(input), keys }
            }
        }
        Plan::Limit { input, offset, count } => {
            let child = optimize(*input);
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
        Plan::UnionAll { left, right } => Plan::UnionAll {
            left: Box::new(optimize(*left)),
            right: Box::new(optimize(*right)),
        },
        Plan::Intersect { left, right } => Plan::Intersect {
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
}
