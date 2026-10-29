use crate::{
    aggregate::AggregateItem,
    expr::Expr,
    join::JoinKind,
    project::Selection,
    sort::SortKey,
};

/// An owned, DataFusion-independent logical query tree.
#[derive(Clone, Debug, PartialEq)]
pub enum Plan {
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
    UnionAll {
        left: Box<Plan>,
        right: Box<Plan>,
    },
    Intersect {
        left: Box<Plan>,
        right: Box<Plan>,
    },
}

impl Plan {
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
