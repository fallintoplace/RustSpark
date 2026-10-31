use std::cmp::Ordering;

use crate::{
    expr::{BinaryOp, Expr},
    scalar::Value,
    statistics::ColumnStatistics,
};

/// Corrupt or internally contradictory bounds cannot prove any predicate false.
fn trustworthy(stats: &ColumnStatistics) -> bool {
    if stats.nulls > stats.rows {
        return false;
    }
    match (&stats.min, &stats.max) {
        (None, None) => true,
        (Some(min), Some(max)) => {
            stats.rows != 0 &&
                min.sql_compare(max).ok().flatten().is_some_and(|order| order != Ordering::Greater)
        }
        _ => false,
    }
}

pub fn can_skip(stats: &ColumnStatistics, column: &str, predicate: &Expr) -> bool {
    if !trustworthy(stats) { return false; }
    if stats.rows == 0 {
        return true;
    }
    match predicate {
        // WHERE FALSE and WHERE NULL cannot return rows regardless of bounds.
        Expr::Literal(Value::Boolean(false) | Value::Null) => true,
        Expr::Not(inner) if matches!(&**inner,
            Expr::Literal(Value::Boolean(true) | Value::Null)) => true,
        Expr::IsNull(inner) if matches!(&**inner, Expr::Column(name) if name.eq_ignore_ascii_case(column)) => {
            stats.nulls == 0
        }
        Expr::Not(inner) if matches!(&**inner, Expr::IsNull(value)
            if matches!(&**value, Expr::Column(name) if name.eq_ignore_ascii_case(column))) => {
            stats.rows == stats.nulls
        }
        Expr::IsDistinctFrom { left, right, negated } => {
            let literal = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name), Expr::Literal(value)) |
                (Expr::Literal(value), Expr::Column(name))
                    if name.eq_ignore_ascii_case(column) => value,
                _ => return false,
            };
            if literal.is_null() {
                return if *negated { stats.nulls == 0 }
                    else { stats.nulls == stats.rows };
            }
            if stats.nulls == stats.rows {
                return *negated;
            }
            let (Some(min), Some(max)) = (&stats.min, &stats.max) else {
                return false;
            };
            let (Some(low), Some(high)) = (
                min.sql_compare(literal).ok().flatten(),
                max.sql_compare(literal).ok().flatten(),
            ) else { return false; };
            if *negated {
                low == Ordering::Greater || high == Ordering::Less
            } else {
                stats.nulls == 0 && low == Ordering::Equal && high == Ordering::Equal
            }
        }
        Expr::Not(inner) => {
            if let Expr::Binary { left, op, right } = inner.as_ref() {
                let inverted = match op {
                    BinaryOp::Equal => Some(BinaryOp::NotEqual),
                    BinaryOp::NotEqual => Some(BinaryOp::Equal),
                    BinaryOp::Less => Some(BinaryOp::GreaterEqual),
                    BinaryOp::LessEqual => Some(BinaryOp::Greater),
                    BinaryOp::Greater => Some(BinaryOp::LessEqual),
                    BinaryOp::GreaterEqual => Some(BinaryOp::Less),
                    _ => None,
                };
                if let Some(op) = inverted {
                    return can_skip(stats, column, &Expr::binary(
                        *left.clone(), op, *right.clone(),
                    ));
                }
            }
            false
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
    // Constant false/unknown predicates do not need any metadata.
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) ||
        matches!(predicate, Expr::Not(inner)
            if matches!(&**inner, Expr::Literal(Value::Boolean(true) | Value::Null))) {
        return true;
    }
    // Never make a skip decision from internally inconsistent summaries.
    if stats.values().any(|summary| !trustworthy(summary)) {
        return false;
    }
    // Inconsistent metadata is not proof that any row can be skipped.
    if let Some(first) = stats.values().next() {
        if stats.values().any(|column| column.rows != first.rows) {
            return false;
        }
    }
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


#[cfg(test)]
mod inconsistent_column_statistics_tests {
    use super::*;

    #[test]
    fn zero_row_metadata_for_another_column_does_not_prune_real_rows() {
        let stats = std::collections::HashMap::from([
            ("missing".into(), ColumnStatistics::default()),
            ("value".into(), ColumnStatistics {
                rows: 5, nulls: 0, min: Some(Value::Integer(1)),
                max: Some(Value::Integer(5)),
            }),
        ]);
        let predicate = Expr::binary(Expr::Column("value".into()),
            BinaryOp::GreaterEqual, Expr::Literal(Value::Integer(2)));
        assert!(!can_skip_columns(&stats, &predicate));
    }
}

#[cfg(test)]
mod constant_false_partition_pruning_tests {
    use super::*;

    #[test]
    fn constants_skip_without_statistics_but_true_never_skips() {
        let empty = std::collections::HashMap::new();
        let false_predicate = Expr::Literal(Value::Boolean(false));
        let null_predicate = Expr::Literal(Value::Null);
        let true_predicate = Expr::Literal(Value::Boolean(true));
        assert!(can_skip_columns(&empty, &false_predicate));
        assert!(can_skip_columns(&empty, &null_predicate));
        assert!(!can_skip_columns(&empty, &true_predicate));
        let not_true = Expr::Not(Box::new(true_predicate.clone()));
        assert!(can_skip_columns(&empty, &not_true));
        let and = Expr::binary(true_predicate.clone(), BinaryOp::And, false_predicate);
        assert!(can_skip_columns(&empty, &and));
        let or = Expr::binary(true_predicate, BinaryOp::Or, null_predicate);
        assert!(!can_skip_columns(&empty, &or));
    }
}

#[cfg(test)]
mod negated_native_range_pruning_tests {
    use super::*;

    #[test]
    fn skips_only_impossible_negations() {
        let summary = ColumnStatistics { rows: 10, nulls: 2,
            min: Some(Value::Integer(10)), max: Some(Value::Integer(19)) };
        let gt = |n| Expr::binary(Expr::Column("x".into()),
            BinaryOp::Greater, Expr::Literal(Value::Integer(n)));
        assert!(can_skip(&summary, "x", &Expr::Not(Box::new(gt(9)))));
        assert!(!can_skip(&summary, "x", &Expr::Not(Box::new(gt(17)))));
        assert!(!can_skip(&summary, "y", &Expr::Not(Box::new(gt(9)))));
        assert!(can_skip(&summary, "x", &Expr::Not(Box::new(
            Expr::binary(Expr::Column("x".into()), BinaryOp::Equal,
                         Expr::Literal(Value::Integer(5)))
        )) ) == false);
    }
}

#[cfg(test)]
mod null_safe_statistics_pruning_tests {
    use super::*;

    fn condition(literal: Value, negated: bool) -> Expr {
        Expr::IsDistinctFrom {
            left: Box::new(Expr::Column("n".into())),
            right: Box::new(Expr::Literal(literal)), negated,
        }
    }

    #[test]
    fn distinct_predicates_prune_only_proven_impossible_partitions() {
        let same = ColumnStatistics { rows: 5, nulls: 0,
            min: Some(Value::Integer(10)), max: Some(Value::Integer(10)) };
        assert!(can_skip(&same, "n", &condition(Value::Integer(10), false)));
        assert!(!can_skip(&same, "n", &condition(Value::Integer(11), false)));
        assert!(can_skip(&same, "n", &condition(Value::Integer(11), true)));
        assert!(!can_skip(&same, "n", &condition(Value::Integer(10), true)));
        let mixed_nulls = ColumnStatistics { rows: 6, nulls: 1, ..same.clone() };
        assert!(!can_skip(&mixed_nulls, "n", &condition(Value::Integer(10), false)));
        let all_null = ColumnStatistics { rows: 4, nulls: 4, min: None, max: None };
        assert!(can_skip(&all_null, "n", &condition(Value::Null, false)));
        assert!(!can_skip(&all_null, "n", &condition(Value::Null, true)));
        assert!(can_skip(&all_null, "n", &condition(Value::Integer(5), true)));
        assert!(can_skip(&same, "n", &condition(Value::Null, true)));
    }
}

#[cfg(test)]
mod corrupt_column_bounds_pruning_tests {
    use super::*;

    #[test]
    fn corrupt_statistics_do_not_discard_matching_rows() {
        let equal = Expr::binary(Expr::Column("x".into()),
            BinaryOp::Equal, Expr::Literal(Value::Integer(15)));
        let reversed = ColumnStatistics {
            rows: 10, nulls: 0,
            min: Some(Value::Integer(20)), max: Some(Value::Integer(10)),
        };
        assert!(!can_skip(&reversed, "x", &equal));
        assert!(!can_skip_columns(&std::collections::HashMap::from([
            ("x".into(), reversed),
        ]), &equal));

        let invalid_nulls = ColumnStatistics {
            rows: 0, nulls: 1, min: None, max: None,
        };
        assert!(!can_skip(&invalid_nulls, "x", &equal));
        let missing_bound = ColumnStatistics {
            rows: 10, nulls: 0, min: Some(Value::Integer(20)), max: None,
        };
        assert!(!can_skip(&missing_bound, "x", &equal));

        let trusted = ColumnStatistics {
            rows: 10, nulls: 0,
            min: Some(Value::Integer(20)), max: Some(Value::Integer(30)),
        };
        assert!(can_skip(&trusted, "x", &equal));
        assert!(can_skip_columns(&std::collections::HashMap::from([
            ("x".into(), trusted),
        ]), &equal));
    }
}
