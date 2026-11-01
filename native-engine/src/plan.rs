use crate::{
    batch::Batch,
    aggregate::AggregateItem,
    expr::Expr,
    join::JoinKind,
    project::Selection,
    sort::SortKey,
};

/// An owned, DataFusion-independent logical query tree.
#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
    /// A bounded inline relation, with validated native schema and values.
    Values { batch: Batch },
    /// Spark's one-row, zero-column relation for SELECT without FROM.
    OneRow,
    Scan {
        table: String,
    },
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },
    Project {
        input: Box<Plan>,
        selections: Vec<Selection>,
    },
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<(Expr, String)>,
        aggregates: Vec<AggregateItem>,
    },
    Sort {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        offset: usize,
        count: Option<usize>,
    },
    Distinct {
        input: Box<Plan>,
    },
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        left_key: String,
        right_key: String,
        kind: JoinKind,
    },
    CrossJoin {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    UnionAll {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Intersect {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    IntersectAll {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    ExceptAll {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Except {
        left: Box<Plan>,
        right: Box<Plan>,
    },
}

impl Plan {
    /// Shared preflight for the materialized and streaming query engines.
    pub(crate) fn validate_expressions(&self) -> crate::Result<()> {
        match self {
            Self::Filter { predicate, .. } => predicate.validate_budget(),
            Self::Project { selections, .. } =>
                selections.iter().try_for_each(|item| item.expression.validate_budget()),
            Self::Sort { keys, .. } =>
                keys.iter().try_for_each(|key| key.expression.validate_budget()),
            Self::Aggregate { group_by, aggregates, .. } => {
                use crate::aggregate::AggregateOp;
                for (expr, _) in group_by { expr.validate_budget()?; }
                for item in aggregates {
                    match &item.op {
                        AggregateOp::CountAll => {}
                        AggregateOp::Count(expr) | AggregateOp::CountDistinct(expr)
                        | AggregateOp::Sum(expr) | AggregateOp::SumDistinct(expr)
                        | AggregateOp::Avg(expr) | AggregateOp::AvgDistinct(expr)
                        | AggregateOp::Min(expr) | AggregateOp::Max(expr)
                        | AggregateOp::First(expr, _) =>
                            expr.validate_budget()?,
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn push_children<'a>(&'a self, pending: &mut Vec<&'a Self>) {
        match self {
            Self::OneRow | Self::Scan { .. } | Self::Values { .. } => {}
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. }
            | Self::Distinct { input } => pending.push(input.as_ref()),
            Self::Join { left, right, .. }
            | Self::CrossJoin { left, right }
            | Self::UnionAll { left, right }
            | Self::Intersect { left, right }
            | Self::IntersectAll { left, right }
            | Self::ExceptAll { left, right }
            | Self::Except { left, right } => {
                pending.push(right.as_ref());
                pending.push(left.as_ref());
            }
        }
    }

    /// Count nodes in an owned native logical tree without recursive walking.
    pub fn operator_count(&self) -> usize {
        let mut pending = vec![self];
        let mut count = 0usize;
        while let Some(plan) = pending.pop() {
            count += 1;
            plan.push_children(&mut pending);
        }
        count
    }

    /// Return all distinct native table scan dependencies in lexical order.
    pub fn scanned_tables(&self) -> Vec<&str> {
        let mut pending = vec![self];
        let mut names = std::collections::BTreeSet::new();
        while let Some(plan) = pending.pop() {
            if let Self::Scan { table } = plan {
                names.insert(table.as_str());
            }
            plan.push_children(&mut pending);
        }
        names.into_iter().collect()
    }

    pub fn scan(table: impl Into<String>) -> Self {
        Self::Scan {
            table: table.into(),
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        Self::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    pub fn project(self, selections: Vec<Selection>) -> Self {
        Self::Project {
            input: Box::new(self),
            selections,
        }
    }

    pub fn limit(self, offset: usize, count: Option<usize>) -> Self {
        Self::Limit {
            input: Box::new(self),
            offset,
            count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_long_left_associative_expressions_before_cloning() {
        use crate::expr::{BinaryOp, Expr};
        use crate::scalar::Value;
        let mut expr = Expr::Literal(Value::Integer(1));
        for _ in 0..100 {
            expr = Expr::binary(expr, BinaryOp::Add, Expr::Literal(Value::Integer(1)));
        }
        let plan = Plan::scan("nonexistent").filter(expr);
        assert!(plan.validate_expressions().is_err());
        assert!(crate::executor::execute(&plan, &crate::catalog::Catalog::new())
            .unwrap_err().to_string().contains("expression"));
    }

    #[test]
    fn plans_remain_owned_and_independent_of_the_catalog() {
        let plan = Plan::scan("sales").limit(2, Some(5));
        assert_eq!(
            plan,
            Plan::Limit {
                input: Box::new(Plan::scan("sales")),
                offset: 2,
                count: Some(5),
            }
        );
    }
}

#[cfg(test)]
mod native_plan_inspection_tests {
    use super::*;

    #[test]
    fn counts_operations_and_deduplicates_scan_dependencies() {
        let plan = Plan::UnionAll {
            left: Box::new(Plan::scan("sales").limit(0, Some(10))),
            right: Box::new(Plan::CrossJoin {
                left: Box::new(Plan::scan("customers")),
                right: Box::new(Plan::scan("sales")),
            }),
        };
        assert_eq!(plan.operator_count(), 6);
        assert_eq!(plan.scanned_tables(), vec!["customers", "sales"]);
        assert_eq!(Plan::scan("alone").operator_count(), 1);
    }
}
