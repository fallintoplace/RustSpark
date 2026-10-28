//! Initial semantics-preserving logical rewrites for native query planning.
use crate::plan::Plan;

fn limit(input: Plan, offset: usize, count: Option<usize>) -> Plan {
    if offset == 0 && count.is_none() { return input; }
    if let Plan::Limit {
        input: inner,
        offset: lower_offset,
        count: lower_count,
    } = input.clone() {
        // Keep invalid limits intact so the executor still reports overflow.
        if lower_count.is_some_and(|n| lower_offset.checked_add(n).is_none())
            || count.is_some_and(|n| offset.checked_add(n).is_none())
        {
            return Plan::Limit { input: Box::new(input), offset, count };
        }
        let advance = lower_count.map_or(offset, |limit| offset.min(limit));
        if let Some(fused_offset) = lower_offset.checked_add(advance) {
            let remaining = lower_count.map(|n| n.saturating_sub(offset));
            let fused_count = match (count, remaining) {
                (Some(outer), Some(inner)) => Some(outer.min(inner)),
                (Some(outer), None) => Some(outer),
                (None, Some(inner)) => Some(inner),
                (None, None) => None,
            };
            if fused_count.is_none_or(|n| fused_offset.checked_add(n).is_some()) {
                return Plan::Limit {
                    input: inner,
                    offset: fused_offset,
                    count: fused_count,
                };
            }
        }
    }
    Plan::Limit { input: Box::new(input), offset, count }
}

/// Rewrite an owned logical plan without depending on DataFusion rules.
/// Only transformations that preserve schema, NULL filtering and row ordering
/// are enabled. Cost-based rewrites belong in a separate phase.
pub fn optimize(plan: &Plan) -> Plan {
    match plan {
        Plan::Scan { .. } => plan.clone(),
        Plan::Filter { input, predicate } => {
            let child = optimize(input);
            match child {
                Plan::Sort { input, keys } => Plan::Sort {
                    input: Box::new(Plan::Filter { input, predicate: predicate.clone() }),
                    keys,
                },
                Plan::Distinct { input } => Plan::Distinct {
                    input: Box::new(Plan::Filter { input, predicate: predicate.clone() }),
                },
                other => Plan::Filter {
                    input: Box::new(other),
                    predicate: predicate.clone(),
                },
            }
        }
        Plan::Project { input, selections } => Plan::Project {
            input: Box::new(optimize(input)),
            selections: selections.clone(),
        },
        Plan::Aggregate { input, group_by, aggregates } => Plan::Aggregate {
            input: Box::new(optimize(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(optimize(input)),
            keys: keys.clone(),
        },
        Plan::Limit { input, offset, count } =>
            limit(optimize(input), *offset, *count),
        Plan::Distinct { input } => Plan::Distinct {
            input: Box::new(optimize(input)),
        },
        Plan::Join { left, right, left_key, right_key, kind } => Plan::Join {
            left: Box::new(optimize(left)),
            right: Box::new(optimize(right)),
            left_key: left_key.clone(),
            right_key: right_key.clone(),
            kind: *kind,
        },
        Plan::UnionAll { left, right } => Plan::UnionAll {
            left: Box::new(optimize(left)),
            right: Box::new(optimize(right)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        batch::Batch, catalog::Catalog,
        executor::execute, expr::{BinaryOp, Expr},
        scalar::Value,
        schema::{DataType, Field, Schema},
        sort::SortKey,
    };

    fn catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.register("events", Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            (0..20).map(|n| vec![Value::Integer(n)]).collect()
        ).unwrap()).unwrap();
        catalog
    }

    #[test]
    fn merges_nested_limits_without_changing_results() {
        let plan = Plan::scan("events").limit(2, Some(8)).limit(3, Some(4));
        assert_eq!(execute(&plan, &catalog()).unwrap(),
                   execute(&optimize(&plan), &catalog()).unwrap());
        assert_eq!(optimize(&plan), Plan::scan("events").limit(5, Some(4)));
    }

    #[test]
    fn pushes_filter_under_sort_without_changing_order() {
        let sort = Plan::Sort {
            input: Box::new(Plan::scan("events")),
            keys: vec![SortKey::ascending(Expr::Column("id".into()))],
        };
        let plan = sort.filter(Expr::binary(
            Expr::Column("id".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(10)),
        ));
        assert_eq!(execute(&plan, &catalog()).unwrap(),
                   execute(&optimize(&plan), &catalog()).unwrap());
        assert!(matches!(optimize(&plan), Plan::Sort { .. }));
    }
}
