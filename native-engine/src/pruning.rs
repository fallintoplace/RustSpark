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

/// Compare borrowed column bounds with a literal. Also handles inverted
/// comparisons without constructing a temporary expression tree.
fn can_skip_binary(
    stats: &ColumnStatistics, column: &str, left: &Expr, op: BinaryOp, right: &Expr,
) -> bool {
    let (literal, op) = match (left, right) {
        (Expr::Column(name), Expr::Literal(value)) if name.eq_ignore_ascii_case(column) => {
            (value, op)
        }
        (Expr::Literal(value), Expr::Column(name)) if name.eq_ignore_ascii_case(column) => {
            let reversed = match op {
                BinaryOp::Less => BinaryOp::Greater,
                BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                BinaryOp::Greater => BinaryOp::Less,
                BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                other => other,
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

// Only constant membership expressions are safe to prove from min/max.
// Unknown or incomparable types keep the row group.
fn can_skip_in_list(
    stats: &ColumnStatistics, column: &str, needle: &Expr,
    choices: &[Expr], negated: bool,
) -> bool {
    let Expr::Column(name) = needle else { return false; };
    if !name.eq_ignore_ascii_case(column) { return false; }
    // Reject computed choices before applying min/max proofs. Iterate the
    // existing expression slice rather than allocating a reference vector
    // for every row group and repeated partition-pruning decision.
    if !choices.iter().all(|choice| matches!(choice, Expr::Literal(_))) {
        return false;
    }
    let literals = || choices.iter().filter_map(|choice| match choice {
        Expr::Literal(value) => Some(value),
        _ => None,
    });
    if stats.rows == stats.nulls { return true; }
    let has_null = literals().any(|value| value.is_null());
    if negated && has_null { return true; }
    if !negated && literals().all(|value| value.is_null()) {
        return true;
    }
    let (Some(min), Some(max)) = (&stats.min, &stats.max) else {
        return false;
    };
    if negated {
        // When every non-NULL value is the same listed literal, NOT IN
        // cannot admit even one row. NULL rows are UNKNOWN.
        if min.sql_compare(max).ok().flatten() != Some(Ordering::Equal) {
            return false;
        }
        literals().filter(|value| !value.is_null()).any(|value|
            min.sql_equal(value).ok().flatten() == Some(true))
    } else {
        // No choice falls inside the inclusive min/max range.
        literals().filter(|value| !value.is_null()).all(|value| {
            match (min.sql_compare(value), max.sql_compare(value)) {
                (Ok(Some(low)), Ok(Some(high))) =>
                    low == Ordering::Greater || high == Ordering::Less,
                _ => false,
            }
        })
    }
}

pub fn can_skip(stats: &ColumnStatistics, column: &str, predicate: &Expr) -> bool {
    // Over-budget trees cannot safely prove a partition empty.
    if predicate.validate_budget().is_err() { return false; }
    can_skip_inner(stats, column, predicate)
}

fn can_skip_inner(stats: &ColumnStatistics, column: &str, predicate: &Expr) -> bool {
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
        Expr::InList { needle, choices, negated } =>
            can_skip_in_list(stats, column, needle, choices, *negated),
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
            if let Expr::InList { needle, choices, negated } = inner.as_ref() {
                return can_skip_in_list(stats, column, needle, choices, !negated);
            }
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
                    return can_skip_binary(stats, column, left, op, right);
                }
            }
            false
        }
        Expr::Binary { left, op: BinaryOp::And, right } => {
            can_skip_inner(stats, column, left) || can_skip_inner(stats, column, right)
        }
        Expr::Binary { left, op: BinaryOp::Or, right } => {
            can_skip_inner(stats, column, left) && can_skip_inner(stats, column, right)
        }
        Expr::Binary { left, op, right } => can_skip_binary(stats, column, left, *op, right),
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

 
/// Identify a simple predicate's column without searching every summary.
/// Complex trees are handled separately by can_skip_columns_inner.
fn simple_predicate_column(predicate: &Expr) -> Option<&str> {
    match predicate {
        Expr::Not(inner) => simple_predicate_column(inner),
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(name) => Some(name),
            _ => None,
        },
        Expr::InList { needle, .. } => match needle.as_ref() {
            Expr::Column(name) => Some(name),
            _ => None,
        },
        Expr::Binary { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(name), Expr::Literal(_))
            | (Expr::Literal(_), Expr::Column(name)) => Some(name),
            _ => None,
        },
        _ => None,
    }
}

/// Conservative pruning across column statistics and boolean predicates.
/// Missing statistics never cause a data partition to be discarded.
/// Conservatively decide whether a boolean tree is impossible using a
/// borrowed expression. Carry NOT as a flag so nested AND/OR predicates do
/// not repeatedly clone entire expression subtrees for De Morgan rewrites.
fn can_skip_columns_inner(
    stats: &std::collections::HashMap<String, ColumnStatistics>,
    predicate: &Expr, negated: bool, mixed_case_keys: bool,
) -> bool {
    match predicate {
        Expr::Literal(Value::Null) => true,
        Expr::Literal(Value::Boolean(value)) => *value == negated,
        Expr::Not(inner) => can_skip_columns_inner(stats, inner, !negated, mixed_case_keys),
        Expr::Binary { left, op: BinaryOp::And, right } => {
            if negated {
                // NOT (A AND B) is NOT A OR NOT B.
                can_skip_columns_inner(stats, left, true, mixed_case_keys)
                    && can_skip_columns_inner(stats, right, true, mixed_case_keys)
            } else {
                can_skip_columns_inner(stats, left, false, mixed_case_keys)
                    || can_skip_columns_inner(stats, right, false, mixed_case_keys)
            }
        }
        Expr::Binary { left, op: BinaryOp::Or, right } => {
            if negated {
                // NOT (A OR B) is NOT A AND NOT B.
                can_skip_columns_inner(stats, left, true, mixed_case_keys)
                    || can_skip_columns_inner(stats, right, true, mixed_case_keys)
            } else {
                can_skip_columns_inner(stats, left, false, mixed_case_keys)
                    && can_skip_columns_inner(stats, right, false, mixed_case_keys)
            }
        }
        _ => {
            // Only the leaf needs inversion. Build it once, not once per
            // column or ancestor in the boolean expression tree.
            let inverse = negated.then(|| Expr::Not(Box::new(predicate.clone())));
            let leaf = inverse.as_ref().unwrap_or(predicate);
            if let Some(name) = simple_predicate_column(leaf) {
                // Normalized schema statistics use lowercase ASCII names.
                // A direct match avoids scanning unrelated columns for every
                // partition and expression leaf.
                if let Some(summary) = stats.get(name) {
                    return can_skip(summary, name, leaf);
                }
                if name.bytes().any(|byte| byte.is_ascii_uppercase()) {
                    let lowered = name.to_ascii_lowercase();
                    if let Some(summary) = stats.get(&lowered) {
                        return can_skip(summary, &lowered, leaf);
                    }
                }
            }
            // External statistics maps may use mixed-case keys. Preserve
            // case-insensitive lookup and conservative fallback behavior.
            mixed_case_keys && stats.iter().any(|(column, summary)| can_skip(summary, column, leaf))
        }
    }
}

/// Conservative pruning across column statistics and boolean predicates.
/// Missing statistics never cause a data partition to be discarded.
pub fn can_skip_columns(
    stats: &std::collections::HashMap<String, ColumnStatistics>,
    predicate: &Expr,
) -> bool {
    if predicate.validate_budget().is_err() { return false; }
    // Constants do not require a statistics lookup, even on empty metadata.
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) ||
        matches!(predicate, Expr::Not(inner)
            if matches!(&**inner, Expr::Literal(Value::Boolean(true) | Value::Null))) {
        return true;
    }
    if stats.values().any(|summary| !trustworthy(summary)) {
        return false;
    }
    if let Some(first) = stats.values().next() {
        if stats.values().any(|column| column.rows != first.rows) {
            return false;
        }
    }
    // Standard Parquet column statistics keys are normalized ASCII lowercase.
    // Avoid a linear scan of every column for each missing predicate leaf.
    // External/mixed-case maps retain the legacy case-insensitive fallback.
    let mixed_case_keys = stats.keys().any(|name|
        name.bytes().any(|byte| byte.is_ascii_uppercase()));
    can_skip_columns_inner(stats, predicate, false, mixed_case_keys)
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

#[cfg(test)]
mod compound_negation_pruning_regressions {
    use super::*;
    use std::collections::HashMap;

    fn compare(name: &str, op: BinaryOp, value: i64) -> Expr {
        Expr::binary(Expr::Column(name.into()), op, Expr::Literal(Value::Integer(value)))
    }
    fn stats() -> HashMap<String, ColumnStatistics> {
        HashMap::from([
            ("x".into(), ColumnStatistics { rows: 10, nulls: 0,
                min: Some(Value::Integer(10)), max: Some(Value::Integer(20)) }),
            ("y".into(), ColumnStatistics { rows: 10, nulls: 0,
                min: Some(Value::Integer(1)), max: Some(Value::Integer(5)) }),
        ])
    }

    #[test]
    fn not_and_can_be_skipped_only_when_every_branch_is_unavoidable() {
        let predicate = Expr::Not(Box::new(Expr::binary(
            compare("x", BinaryOp::GreaterEqual, 10), BinaryOp::And,
            compare("y", BinaryOp::LessEqual, 5),
        )));
        assert!(can_skip_columns(&stats(), &predicate));
        let with_unknown_y = Expr::Not(Box::new(Expr::binary(
            compare("x", BinaryOp::GreaterEqual, 10), BinaryOp::And,
            compare("missing", BinaryOp::LessEqual, 5),
        )));
        assert!(!can_skip_columns(&stats(), &with_unknown_y));
    }

    #[test]
    fn not_or_prunes_if_one_disjunct_is_provably_true() {
        let impossible = Expr::Not(Box::new(Expr::binary(
            compare("x", BinaryOp::GreaterEqual, 10), BinaryOp::Or,
            compare("y", BinaryOp::Greater, 5),
        )));
        assert!(can_skip_columns(&stats(), &impossible));
        let possible = Expr::Not(Box::new(Expr::binary(
            compare("x", BinaryOp::Less, 0), BinaryOp::Or,
            compare("y", BinaryOp::Greater, 5),
        )));
        assert!(!can_skip_columns(&stats(), &possible));
    }
}

#[cfg(test)]
mod borrowed_boolean_pruning_regressions {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn deeply_nested_boolean_trees_match_row_evaluation() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, true)]).unwrap(),
            (10..20).map(|n| vec![Value::Integer(n)]).chain([vec![Value::Null]]).collect()
        ).unwrap();
        let summary = ColumnStatistics::from_batch(&batch, "x").unwrap();
        let stats = std::collections::HashMap::from([("x".to_string(), summary)]);
        let above = Expr::binary(
            Expr::Column("x".into()), BinaryOp::GreaterEqual,
            Expr::Literal(Value::Integer(10))
        );
        let below = Expr::binary(
            Expr::Column("x".into()), BinaryOp::Less,
            Expr::Literal(Value::Integer(10))
        );
        let mut impossible = below.clone();
        for _ in 0..80 {
            impossible = Expr::binary(impossible, BinaryOp::And, above.clone());
        }
        let predicates = vec![
            impossible,
            Expr::Not(Box::new(Expr::binary(above.clone(), BinaryOp::Or, below.clone()))),
            Expr::Not(Box::new(Expr::Not(Box::new(below)))),
            Expr::Not(Box::new(Expr::binary(above, BinaryOp::And,
                Expr::Literal(Value::Boolean(false))))),
        ];
        for predicate in predicates {
            let matches = (0..batch.num_rows()).any(|row|
                predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true));
            if can_skip_columns(&stats, &predicate) {
                assert!(!matches, "pruning discarded a matching row: {predicate:?}");
            }
        }
    }

    #[test]
    fn boolean_literals_preserve_empty_metadata_shortcuts() {
        let stats = std::collections::HashMap::new();
        assert!(can_skip_columns(&stats, &Expr::Literal(Value::Boolean(false))));
        assert!(can_skip_columns(&stats, &Expr::Not(Box::new(
            Expr::Literal(Value::Boolean(true))))));
        assert!(!can_skip_columns(&stats, &Expr::Not(Box::new(
            Expr::Literal(Value::Boolean(false))))));
    }
}

#[cfg(test)]
mod nested_partition_pruning_preflight_tests {
    use super::*;
    use std::collections::HashMap;

    fn summary() -> ColumnStatistics {
        ColumnStatistics { rows: 50, nulls: 0,
            min: Some(Value::Integer(10)), max: Some(Value::Integer(19)) }
    }
    fn gt(column: &str, threshold: i64) -> Expr {
        Expr::binary(Expr::Column(column.into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(threshold)))
    }

    #[test]
    fn deeply_nested_boolean_predicates_preserve_three_valued_pruning() {
        let stats = HashMap::from([
            ("a".to_string(), summary()),
            ("b".to_string(), summary()),
            ("c".to_string(), summary()),
        ]);
        let mut impossible = gt("a", 30);
        for index in 0..96 {
            impossible = Expr::binary(impossible, BinaryOp::Or,
                gt(if index % 2 == 0 { "b" } else { "c" }, 50));
        }
        assert!(can_skip_columns(&stats, &impossible));
        let possible = Expr::binary(impossible, BinaryOp::Or, gt("missing", 1));
        assert!(!can_skip_columns(&stats, &possible));
        let corrupt = HashMap::from([
            ("a".into(), summary()),
            ("b".into(), ColumnStatistics { rows: 49, ..summary() }),
        ]);
        assert!(!can_skip_columns(&corrupt, &gt("a", 30)));
        assert!(can_skip_columns(&HashMap::new(),
            &Expr::Literal(Value::Boolean(false))));
    }

    #[test]
    fn impossible_constant_branch_prunes_even_without_statistics() {
        let stats = HashMap::new();
        let predicate = Expr::binary(
            Expr::Literal(Value::Boolean(true)), BinaryOp::And,
            Expr::Literal(Value::Boolean(false)),
        );
        assert!(can_skip_columns(&stats, &predicate));
        let predicate = Expr::binary(
            Expr::Literal(Value::Boolean(false)), BinaryOp::Or,
            Expr::Literal(Value::Boolean(true)),
        );
        assert!(!can_skip_columns(&stats, &predicate));
    }

    #[test]
    fn compound_negations_remain_equivalent_to_demorgan_rewrites() {
        let stats = HashMap::from([("a".to_string(), summary())]);
        let left = gt("a", 9);
        let right = gt("a", 20);
        let negated = Expr::Not(Box::new(Expr::binary(
            left.clone(), BinaryOp::Or, right.clone(),
        )));
        let rewritten = Expr::binary(
            Expr::Not(Box::new(left)), BinaryOp::And,
            Expr::Not(Box::new(right)),
        );
        assert_eq!(can_skip_columns(&stats, &negated),
            can_skip_columns(&stats, &rewritten));
    }
}

#[cfg(test)]
mod borrowed_negated_comparisons_tests {
    use super::*;

    #[test]
    fn not_comparisons_match_their_direct_inverses() {
        let stats = ColumnStatistics {
            rows: 30,
            nulls: 2,
            min: Some(Value::Integer(10)),
            max: Some(Value::Integer(20)),
        };
        for (op, inverse) in [
            (BinaryOp::Equal, BinaryOp::NotEqual),
            (BinaryOp::NotEqual, BinaryOp::Equal),
            (BinaryOp::Less, BinaryOp::GreaterEqual),
            (BinaryOp::LessEqual, BinaryOp::Greater),
            (BinaryOp::Greater, BinaryOp::LessEqual),
            (BinaryOp::GreaterEqual, BinaryOp::Less),
        ] {
            for value in [9, 10, 15, 20, 21] {
                for reversed in [false, true] {
                    let column = Expr::Column("value".into());
                    let literal = Expr::Literal(Value::Integer(value));
                    let original = if reversed {
                        Expr::binary(literal.clone(), op, column.clone())
                    } else {
                        Expr::binary(column.clone(), op, literal.clone())
                    };
                    let inverted = if reversed {
                        Expr::binary(literal, inverse, column)
                    } else {
                        Expr::binary(column, inverse, literal)
                    };
                    assert_eq!(
                        can_skip(&stats, "value", &Expr::Not(Box::new(original))),
                        can_skip(&stats, "value", &inverted),
                        "operator={op:?} value={value} reversed={reversed}"
                    );
                }
            }
        }
    }

    #[test]
    fn missing_columns_and_nullable_bounds_remain_conservative() {
        let stats = ColumnStatistics {
            rows: 10,
            nulls: 10,
            min: None,
            max: None,
        };
        let comparison = Expr::binary(
            Expr::Column("x".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(5)),
        );
        assert!(can_skip(&stats, "x", &Expr::Not(Box::new(comparison.clone()))));
        assert!(!can_skip(&stats, "y", &Expr::Not(Box::new(comparison))));
    }
}

#[cfg(test)]
mod membership_pruning_differential_tests {
    use super::*;
    use crate::{batch::Batch, schema::{DataType, Field, Schema}};

    #[test]
    fn never_prunes_a_row_group_with_any_matching_row() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        for rows in [
            vec![Value::Null, Value::Integer(10), Value::Integer(19)],
            vec![Value::Null, Value::Null],
            vec![Value::Integer(42), Value::Integer(42), Value::Null],
            vec![Value::Integer(-5), Value::Integer(0), Value::Integer(8)],
            vec![Value::Integer(i64::MIN), Value::Integer(i64::MAX)],
        ] {
            let input = Batch::from_rows(schema.clone(),
                rows.into_iter().map(|v| vec![v]).collect()).unwrap();
            let summary = ColumnStatistics::from_batch(&input, "n").unwrap();
            for choices in [
                vec![], vec![Value::Null], vec![Value::Integer(0)],
                vec![Value::Integer(10), Value::Integer(42)],
                vec![Value::Integer(9), Value::Integer(20), Value::Null],
                vec![Value::Integer(i64::MIN), Value::Integer(i64::MAX)],
            ] {
                for negated in [false, true] {
                    let predicate = Expr::InList {
                        needle: Box::new(Expr::Column("n".into())),
                        choices: choices.iter().cloned().map(Expr::Literal).collect(),
                        negated,
                    };
                    let matching = (0..input.num_rows()).any(|row|
                        predicate.evaluate(&input, row).unwrap()
                            .as_boolean().unwrap() == Some(true));
                    assert!(!matching || !can_skip(&summary, "n", &predicate),
                        "pruned a match: {predicate:?}, {summary:?}");
                    let inverse = Expr::Not(Box::new(predicate.clone()));
                    let inverted_matches = (0..input.num_rows()).any(|row|
                        inverse.evaluate(&input, row).unwrap()
                            .as_boolean().unwrap() == Some(true));
                    assert!(!inverted_matches || !can_skip(&summary, "n", &inverse));
                }
            }
        }
    }

    #[test]
    fn prunes_out_of_range_null_and_single_value_memberships() {
        let stats = ColumnStatistics {
            rows: 10, nulls: 2,
            min: Some(Value::Integer(10)), max: Some(Value::Integer(19)),
        };
        let make = |values: Vec<Value>, negated| Expr::InList {
            needle: Box::new(Expr::Column("n".into())),
            choices: values.into_iter().map(Expr::Literal).collect(),
            negated,
        };
        assert!(can_skip(&stats, "n", &make(
            vec![Value::Integer(1), Value::Integer(30)], false)));
        assert!(!can_skip(&stats, "n", &make(vec![Value::Integer(15)], false)));
        assert!(can_skip(&stats, "n", &make(
            vec![Value::Integer(15), Value::Null], true)));
        let point = ColumnStatistics {
            rows: 3, nulls: 1,
            min: Some(Value::Integer(42)), max: Some(Value::Integer(42)),
        };
        assert!(can_skip(&point, "n", &make(vec![Value::Integer(42)], true)));
        assert!(!can_skip(&point, "n", &make(vec![Value::Integer(41)], true)));
    }
}

#[cfg(test)]
mod direct_statistics_lookup_regressions {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn wide_statistics_maps_prune_only_proven_impossible_ranges() {
        let mut stats = (0..1500).map(|index| {
            (format!("unused_{index}"), ColumnStatistics {
                rows: 20, nulls: 0,
                min: Some(Value::Integer(0)),
                max: Some(Value::Integer(100)),
            })
        }).collect::<HashMap<_, _>>();
        stats.insert("revenue".into(), ColumnStatistics {
            rows: 20, nulls: 0,
            min: Some(Value::Integer(1000)),
            max: Some(Value::Integer(2000)),
        });
        for name in ["revenue", "Revenue", "REVENUE"] {
            let predicate = Expr::binary(Expr::Column(name.into()),
                BinaryOp::Less, Expr::Literal(Value::Integer(1000)));
            assert!(can_skip_columns(&stats, &predicate));
            let possible = Expr::binary(Expr::Column(name.into()),
                BinaryOp::LessEqual, Expr::Literal(Value::Integer(1000)));
            assert!(!can_skip_columns(&stats, &possible));
        }
        let missing = Expr::binary(Expr::Column("unknown".into()),
            BinaryOp::Equal, Expr::Literal(Value::Integer(5)));
        assert!(!can_skip_columns(&stats, &missing));
    }

    #[test]
    fn null_and_null_safe_leaves_preserve_case_insensitive_pruning() {
        let stats = HashMap::from([("Value".to_string(), ColumnStatistics {
            rows: 10, nulls: 0,
            min: Some(Value::Integer(1)),
            max: Some(Value::Integer(1)),
        })]);
        let is_null = Expr::IsNull(Box::new(Expr::Column("value".into())));
        assert!(can_skip_columns(&stats, &is_null));
        let distinct = Expr::IsDistinctFrom {
            left: Box::new(Expr::Literal(Value::Integer(1))),
            right: Box::new(Expr::Column("VALUE".into())),
            negated: false,
        };
        assert!(can_skip_columns(&stats, &distinct));
        assert!(!can_skip_columns(&stats, &Expr::Not(Box::new(is_null))));
    }
}

#[cfg(test)]
mod normalized_pruning_lookup_tests {
    use super::*;

    fn impossible(name: &str) -> Expr {
        Expr::binary(
            Expr::Column(name.to_owned()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(1000)),
        )
    }

    #[test]
    fn normalized_wide_summaries_preserve_boolean_pruning() {
        let summary = ColumnStatistics {
            rows: 100, nulls: 1,
            min: Some(Value::Integer(1)),
            max: Some(Value::Integer(20)),
        };
        let mut columns = std::collections::HashMap::new();
        for index in 0..1500 {
            columns.insert(format!("field_{index}"), summary.clone());
        }
        let missing = impossible("unknown_field");
        let existing = impossible("FIELD_1499");
        let disjunction = Expr::binary(existing.clone(), BinaryOp::Or, missing.clone());
        assert!(!can_skip_columns(&columns, &disjunction));
        let conjunction = Expr::binary(existing.clone(), BinaryOp::And, missing);
        assert!(can_skip_columns(&columns, &conjunction));
        assert!(can_skip_columns(&columns, &existing));
        columns.insert("MiXeD".to_owned(), summary);
        assert!(can_skip_columns(&columns, &impossible("mixed")));
        assert!(can_skip_columns(&columns, &impossible("FIELD_1499")));
        assert!(!can_skip_columns(&columns, &impossible("absent")));
    }

    #[test]
    fn contradictory_and_unknown_metadata_do_not_prune() {
        let valid = ColumnStatistics {
            rows: 5, nulls: 0,
            min: Some(Value::Integer(1)), max: Some(Value::Integer(3)),
        };
        let mut stats = std::collections::HashMap::from([("FIELD".into(), valid.clone())]);
        assert!(can_skip_columns(&stats, &impossible("field")));
        stats.insert("other".into(), ColumnStatistics { rows: 6, ..valid });
        assert!(!can_skip_columns(&stats, &impossible("field")));
    }
}

#[cfg(test)]
mod allocation_free_inlist_pruning_regressions {
    use super::*;

    #[test]
    fn wide_literal_lists_prune_without_changing_three_valued_logic() {
        let stats = ColumnStatistics {
            rows: 10, nulls: 1,
            min: Some(Value::Integer(100)), max: Some(Value::Integer(120)),
        };
        let predicate = |values: Vec<Expr>, negated| Expr::InList {
            needle: Box::new(Expr::Column("n".into())),
            choices: values,
            negated,
        };
        let outside = (0..10_000).map(|n|
            Expr::Literal(Value::Integer(n + 1_000))
        ).collect::<Vec<_>>();
        assert!(can_skip(&stats, "n", &predicate(outside.clone(), false)));
        let mut inside = outside;
        inside.push(Expr::Literal(Value::Integer(110)));
        assert!(!can_skip(&stats, "n", &predicate(inside, false)));
        assert!(can_skip(&stats, "n", &predicate(
            vec![Expr::Literal(Value::Null), Expr::Literal(Value::Integer(110))], true,
        )));
        assert!(!can_skip(&stats, "n", &predicate(
            vec![Expr::Column("other".into())], false,
        )));
    }
}

#[cfg(test)]
mod normalized_membership_pruning_tests {
    use super::*;
    use std::collections::HashMap;

    fn summaries(min: i64, max: i64) -> HashMap<String, ColumnStatistics> {
        HashMap::from([("account_id".into(), ColumnStatistics {
            rows: 100, nulls: 3,
            min: Some(Value::Integer(min)),
            max: Some(Value::Integer(max)),
        })])
    }

    fn membership(values: &[i64], negated: bool) -> Expr {
        Expr::InList {
            needle: Box::new(Expr::Column("account_id".into())),
            choices: values.iter().map(|&value| Expr::Literal(Value::Integer(value))).collect(),
            negated,
        }
    }

    #[test]
    fn normalized_stats_prune_impossible_in_lists_without_scanning_all_columns() {
        let stats = summaries(10, 20);
        assert!(can_skip_columns(&stats, &membership(&[1, 2, 30], false)));
        assert!(!can_skip_columns(&stats, &membership(&[1, 10, 30], false)));
        assert!(!can_skip_columns(&stats, &membership(&[15], true)));
        assert!(can_skip_columns(&summaries(15, 15), &membership(&[15], true)));
    }

    #[test]
    fn nulls_and_boolean_composition_preserve_conservative_pruning() {
        let stats = summaries(10, 20);
        let impossible = membership(&[1, 2], false);
        let possible = membership(&[12], false);
        assert!(can_skip_columns(&stats, &Expr::binary(
            impossible.clone(), BinaryOp::And, possible.clone()
        )));
        assert!(!can_skip_columns(&stats, &Expr::binary(
            impossible, BinaryOp::Or, possible
        )));
        let mut missing = HashMap::new();
        missing.insert("other".into(), stats["account_id"].clone());
        assert!(!can_skip_columns(&missing, &membership(&[1, 2], false)));
    }
}

#[cfg(test)]
mod direct_pruning_expression_budget_tests {
    use super::*;

    #[test]
    fn deep_boolean_trees_fall_back_to_safe_no_prune() {
        let summary = ColumnStatistics {
            rows: 4, nulls: 0,
            min: Some(Value::Integer(1)), max: Some(Value::Integer(4)),
        };
        let mut predicate = Expr::Literal(Value::Boolean(false));
        for _ in 0..80 {
            predicate = Expr::binary(predicate, BinaryOp::And,
                Expr::Literal(Value::Boolean(false)));
        }
        assert!(!can_skip(&summary, "n", &predicate));
        let stats = std::collections::HashMap::from([("n".into(), summary)]);
        assert!(!can_skip_columns(&stats, &predicate));
        assert!(can_skip(&stats["n"], "n",
            &Expr::Literal(Value::Boolean(false))));
    }
}
