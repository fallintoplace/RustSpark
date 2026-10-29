use std::cmp::Ordering;

use crate::{
    expr::{BinaryOp, Expr},
    scalar::Value,
    statistics::ColumnStatistics,
};

pub fn can_skip(stats: &ColumnStatistics, column: &str, predicate: &Expr) -> bool {
    if stats.rows == 0 {
        return true;
    }
    match predicate {
        Expr::IsNull(inner) if matches!(&**inner, Expr::Column(name) if name.eq_ignore_ascii_case(column)) => {
            stats.nulls == 0
        }
        Expr::Not(inner) if matches!(&**inner, Expr::IsNull(value)
            if matches!(&**value, Expr::Column(name) if name.eq_ignore_ascii_case(column))) => {
            stats.rows == stats.nulls
        }
        Expr::Binary { left, op: BinaryOp::And, right } => {
            can_skip(stats, column, left) || can_skip(stats, column, right)
        }
        Expr::Binary { left, op: BinaryOp::Or, right } => {
            can_skip(stats, column, left) && can_skip(stats, column, right)
        }
        Expr::Binary { left, op, right } => {
            let (literal, op) = match (&**left, &**right) {
                (Expr::Column(name), Expr::Literal(value)) if name.eq_ignore_ascii_case(column) => (value, *op),
                (Expr::Literal(value), Expr::Column(name)) if name.eq_ignore_ascii_case(column) => {
                    let reversed = match op {
                        BinaryOp::Less => BinaryOp::Greater,
                        BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                        BinaryOp::Greater => BinaryOp::Less,
                        BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                        other => *other,
                    };
                    (value, reversed)
                }
                _ => return false,
            };
            if literal.is_null() || stats.rows == stats.nulls {
                return true;
            }
            let (Some(min), Some(max)) = (&stats.min, &stats.max) else {
                return false;
            };
            let compare = |value: &Value| value.sql_compare(literal).ok().flatten();
            let (Some(low), Some(high)) = (compare(min), compare(max)) else {
                return false;
            };
            match op {
                BinaryOp::Equal => low == Ordering::Greater || high == Ordering::Less,
                BinaryOp::NotEqual => low == Ordering::Equal && high == Ordering::Equal,
                BinaryOp::Less => low != Ordering::Less,
                BinaryOp::LessEqual => low == Ordering::Greater,
                BinaryOp::Greater => high != Ordering::Greater,
                BinaryOp::GreaterEqual => high == Ordering::Less,
                _ => false,
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats() -> ColumnStatistics {
        ColumnStatistics {
            rows: 30, nulls: 2,
            min: Some(Value::Integer(10)),
            max: Some(Value::Integer(19)),
        }
    }

    fn expr(op: BinaryOp, value: i64) -> Expr {
        Expr::binary(
            Expr::Column("x".into()), op,
            Expr::Literal(Value::Integer(value)),
        )
    }

    #[test]
    fn rejects_impossible_ranges_and_preserves_possible_matches() {
        assert!(can_skip(&stats(), "x", &expr(BinaryOp::Equal, 3)));
        assert!(can_skip(&stats(), "x", &expr(BinaryOp::Greater, 19)));
        assert!(!can_skip(&stats(), "x", &expr(BinaryOp::GreaterEqual, 19)));
        assert!(!can_skip(&stats(), "x", &expr(BinaryOp::Equal, 12)));
        assert!(!can_skip(&stats(), "y", &expr(BinaryOp::Equal, 3)));
    }

    #[test]
    fn null_predicates_follow_three_valued_logic() {
        assert!(!can_skip(&stats(), "x", &Expr::IsNull(Box::new(Expr::Column("x".into())))));
        assert!(can_skip(&stats(), "x", &expr(BinaryOp::Equal, 1_000)));
        assert!(can_skip(&stats(), "x", &expr(BinaryOp::Equal, 2)));
        let all_null = ColumnStatistics { rows: 5, nulls: 5, min: None, max: None };
        assert!(can_skip(&all_null, "x", &expr(BinaryOp::Equal, 5)));
    }
}

 
/// Conservative pruning across column statistics and boolean predicates.
/// Missing statistics never cause a data partition to be discarded.
pub fn can_skip_columns(
    stats: &std::collections::HashMap<String, ColumnStatistics>,
    predicate: &Expr,
) -> bool {
    match predicate {
        Expr::Binary { left, op: BinaryOp::And, right } =>
            can_skip_columns(stats, left) || can_skip_columns(stats, right),
        Expr::Binary { left, op: BinaryOp::Or, right } =>
            can_skip_columns(stats, left) && can_skip_columns(stats, right),
        _ => stats.iter().any(|(column, summary)| can_skip(summary, column, predicate)),
    }
}

#[cfg(test)]
mod compound_pruning_feature_tests {
    use super::*;

    #[test]
    fn impossible_disjunctions_prune_but_missing_stats_do_not() {
        let summary = ColumnStatistics {
            rows: 10, nulls: 0, min: Some(Value::Integer(10)),
            max: Some(Value::Integer(20)),
        };
        let stats = std::collections::HashMap::from([
            ("x".to_owned(), summary.clone()),
            ("y".to_owned(), summary),
        ]);
        let greater = |name: &str, n| Expr::binary(Expr::Column(name.into()),
            BinaryOp::Greater, Expr::Literal(Value::Integer(n)));
        let impossible = Expr::binary(greater("x", 50), BinaryOp::Or, greater("y", 100));
        assert!(can_skip_columns(&stats, &impossible));
        let possibly_matching = Expr::binary(greater("x", 50), BinaryOp::Or, greater("z", 1));
        assert!(!can_skip_columns(&stats, &possibly_matching));
        let and = Expr::binary(greater("x", 50), BinaryOp::And, greater("z", 1));
        assert!(can_skip_columns(&stats, &and));
    }
}

