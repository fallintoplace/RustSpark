//! Typed filter fast path over native column buffers with SQL-null semantics.
use crate::{batch::Batch, expr::{BinaryOp, Expr}, scalar::Value, schema::DataType, Error, Result};

fn integer_comparison(op: BinaryOp, left: i64, right: i64) -> Option<bool> {
    Some(match op {
        BinaryOp::Equal => left == right,
        BinaryOp::NotEqual => left != right,
        BinaryOp::Less => left < right,
        BinaryOp::LessEqual => left <= right,
        BinaryOp::Greater => left > right,
        BinaryOp::GreaterEqual => left >= right,
        _ => return None,
    })
}

fn fast_text_comparison<'a>(
    batch: &'a Batch, predicate: &'a Expr,
) -> Result<Option<(&'a [Value], BinaryOp, &'a str)>> {
    if let Expr::Binary { left, op, right } = predicate {
        if matches!(op, BinaryOp::Equal | BinaryOp::NotEqual |
            BinaryOp::Less | BinaryOp::LessEqual |
            BinaryOp::Greater | BinaryOp::GreaterEqual) {
            let (name, op, value) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(name), Expr::Literal(Value::Text(value))) =>
                    (name, *op, value),
                (Expr::Literal(Value::Text(value)), Expr::Column(name)) => {
                    let reversed = match op {
                        BinaryOp::Less => BinaryOp::Greater,
                        BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                        BinaryOp::Greater => BinaryOp::Less,
                        BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                        other => *other,
                    };
                    (name, reversed, value)
                },
                _ => return Ok(None),
            };
            let index = batch.schema().column(name)?;
            if batch.schema().field(index)?.data_type == DataType::Text {
                return Ok(Some((batch.column(index)?, op, value)));
            }
        }
    }
    Ok(None)
}

fn fast_boolean_column<'a>(batch: &'a Batch, predicate: &Expr) -> Result<Option<&'a [Value]>> {
    let Expr::Column(name) = predicate else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type == DataType::Boolean {
        Ok(Some(batch.column(index)?))
    } else {
        Ok(None)
    }
}

// Directly evaluate two Boolean input columns in one pass. For WHERE,
// a row is selected only when the Boolean expression is TRUE; NULL
// contributes UNKNOWN and is excluded unless the other OR operand is TRUE.
fn fast_boolean_pair<'a>(
    batch: &'a Batch, predicate: &Expr, boolean_op: BinaryOp,
) -> Result<Option<(&'a [Value], &'a [Value])>> {
    let Expr::Binary { left, op, right } = predicate else { return Ok(None); };
    if *op != boolean_op { return Ok(None); }
    let Some(first) = fast_boolean_column(batch, left)? else { return Ok(None) };
    let Some(second) = fast_boolean_column(batch, right)? else { return Ok(None) };
    Ok(Some((first, second)))
}

fn fast_float_predicate<'a>(
    batch: &'a Batch,
    predicate: &'a Expr,
) -> Result<Option<(&'a [Value], BinaryOp, f64)>> {
    if let Expr::Binary { left, op, right } = predicate {
        let (column, op, value) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(name), Expr::Literal(Value::Float(value))) =>
                (name, *op, *value),
            (Expr::Literal(Value::Float(value)), Expr::Column(name)) => {
                let reversed = match op {
                    BinaryOp::Less => BinaryOp::Greater,
                    BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                    BinaryOp::Greater => BinaryOp::Less,
                    BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                    other => *other,
                };
                (name, reversed, *value)
            }
            _ => return Ok(None),
        };
        if integer_comparison(op, 0, 0).is_some() {
            let index = batch.schema().column(column)?;
            if batch.schema().field(index)?.data_type == DataType::Float {
                return Ok(Some((batch.column(index)?, op, value)));
            }
        }
    }
    Ok(None)
}

fn fast_integer_predicate<'a>(
    batch: &'a Batch,
    predicate: &'a Expr,
) -> Result<Option<(&'a [Value], BinaryOp, i64)>> {
    let Expr::Binary { left, op, right } = predicate else {
        return Ok(None);
    };
    let (name, op, value) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(name), Expr::Literal(Value::Integer(value))) =>
            (name, *op, *value),
        (Expr::Literal(Value::Integer(value)), Expr::Column(name)) => {
            let reversed = match op {
                BinaryOp::Less => BinaryOp::Greater,
                BinaryOp::LessEqual => BinaryOp::GreaterEqual,
                BinaryOp::Greater => BinaryOp::Less,
                BinaryOp::GreaterEqual => BinaryOp::LessEqual,
                other => *other,
            };
            (name, reversed, *value)
        }
        _ => return Ok(None),
    };
    if integer_comparison(op, 0, 0).is_none() {
        return Ok(None);
    }
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type == crate::schema::DataType::Integer {
        return Ok(Some((batch.column(index)?, op, value)));
    }
    Ok(None)
}

/// Fuse two comparisons on the same integer column into one vector scan.
/// All other predicates retain the scalar SQL fallback.
fn fast_integer_pair<'a>(
    batch: &'a Batch, predicate: &'a Expr, boolean_op: BinaryOp,
) -> Result<Option<(&'a [Value], (BinaryOp, i64), (BinaryOp, i64))>> {
    let Expr::Binary { left, op, right } = predicate else { return Ok(None); };
    if *op != boolean_op { return Ok(None); }
    let Some((left_values, left_op, left_value)) =
        fast_integer_predicate(batch, left)? else { return Ok(None); };
    let Some((right_values, right_op, right_value)) =
        fast_integer_predicate(batch, right)? else { return Ok(None); };
    if !std::ptr::eq(left_values, right_values) { return Ok(None); }
    Ok(Some((left_values, (left_op, left_value), (right_op, right_value))))
}

fn fast_integer_conjunction<'a>(
    batch: &'a Batch, predicate: &'a Expr,
) -> Result<Option<(&'a [Value], (BinaryOp, i64), (BinaryOp, i64))>> {
    fast_integer_pair(batch, predicate, BinaryOp::And)
}

fn fast_integer_disjunction<'a>(
    batch: &'a Batch, predicate: &'a Expr,
) -> Result<Option<(&'a [Value], (BinaryOp, i64), (BinaryOp, i64))>> {
    fast_integer_pair(batch, predicate, BinaryOp::Or)
}

fn fast_null_test<'a>(batch: &'a Batch, predicate: &Expr)
    -> Result<Option<(&'a [Value], bool, bool)>>
{
    let (inner, select_null) = match predicate {
        Expr::IsNull(inner) => (inner.as_ref(), true),
        Expr::Not(inner) => match inner.as_ref() {
            Expr::IsNull(column) => (column.as_ref(), false),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let Expr::Column(name) = inner else { return Ok(None) };
    let index = batch.schema().column(name)?;
    let nullable = batch.schema().field(index)?.nullable;
    Ok(Some((batch.column(index)?, select_null, nullable)))
}

/// Prebuild an integer literal IN-list once per input batch. Scalar IN
/// evaluates every choice for every row, which is quadratic in row and list
/// counts. Mixed numeric types and computed choices keep the SQL fallback.
fn fast_integer_in_list<'a>(
    batch: &'a Batch, predicate: &Expr,
) -> Result<Option<(&'a [Value], std::collections::HashSet<i64>, bool, bool)>> {
    let Expr::InList { needle, choices, negated } = predicate else {
        return Ok(None);
    };
    let Expr::Column(name) = needle.as_ref() else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type != DataType::Integer {
        return Ok(None);
    }
    let mut keys = std::collections::HashSet::with_capacity(choices.len());
    let mut has_null = false;
    for choice in choices {
        match choice {
            Expr::Literal(Value::Integer(value)) => { keys.insert(*value); },
            Expr::Literal(Value::Null) => has_null = true,
            _ => return Ok(None),
        }
    }
    Ok(Some((batch.column(index)?, keys, *negated, has_null)))
}

/// Resolve constant TEXT IN lists once and compare borrowed column values.
/// NULL choices make unmatched NOT IN rows UNKNOWN, not TRUE.
fn fast_text_in_list<'a>(
    batch: &'a Batch, predicate: &'a Expr,
) -> Result<Option<(&'a [Value], std::collections::HashSet<&'a str>, bool, bool)>> {
    let Expr::InList { needle, choices, negated } = predicate else {
        return Ok(None);
    };
    let Expr::Column(name) = needle.as_ref() else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type != DataType::Text {
        return Ok(None);
    }
    let mut keys = std::collections::HashSet::with_capacity(choices.len());
    let mut has_null = false;
    for choice in choices {
        match choice {
            Expr::Literal(Value::Text(value)) => { keys.insert(value.as_str()); },
            Expr::Literal(Value::Null) => has_null = true,
            _ => return Ok(None),
        }
    }
    Ok(Some((batch.column(index)?, keys, *negated, has_null)))
}

// Spark orders NaN after finite values, but treats NaNs as equal and
// signed zero as equal in IN membership. Match Value::sql_compare exactly.
fn float_membership_key(number: f64) -> u64 {
    if number.is_nan() { f64::NAN.to_bits() }
    else if number == 0.0 { 0.0f64.to_bits() }
    else { number.to_bits() }
}

fn fast_float_in_list<'a>(
    batch: &'a Batch, predicate: &Expr,
) -> Result<Option<(&'a [Value], std::collections::HashSet<u64>, bool, bool)>> {
    let Expr::InList { needle, choices, negated } = predicate else {
        return Ok(None);
    };
    let Expr::Column(name) = needle.as_ref() else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type != DataType::Float {
        return Ok(None);
    }
    let mut keys = std::collections::HashSet::with_capacity(choices.len());
    let mut has_null = false;
    for choice in choices {
        match choice {
            Expr::Literal(Value::Float(value)) => {
                keys.insert(float_membership_key(*value));
            },
            Expr::Literal(Value::Null) => has_null = true,
            // Mixed INTEGER/DOUBLE comparisons use exact SQL comparisons
            // instead of rounding integers into DOUBLE keys.
            _ => return Ok(None),
        }
    }
    Ok(Some((batch.column(index)?, keys, *negated, has_null)))
}

/// A BOOLEAN domain has only two non-NULL values. Replace repeated
/// literal comparisons with two membership flags for the whole batch.
fn fast_boolean_in_list<'a>(
    batch: &'a Batch, predicate: &Expr,
) -> Result<Option<(&'a [Value], [bool; 2], bool, bool)>> {
    let Expr::InList { needle, choices, negated } = predicate else {
        return Ok(None);
    };
    let Expr::Column(name) = needle.as_ref() else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type != DataType::Boolean {
        return Ok(None);
    }
    let mut matched = [false; 2];
    let mut has_null = false;
    for choice in choices {
        match choice {
            Expr::Literal(Value::Boolean(value)) => matched[usize::from(*value)] = true,
            Expr::Literal(Value::Null) => has_null = true,
            _ => return Ok(None),
        }
    }
    Ok(Some((batch.column(index)?, matched, *negated, has_null)))
}

/// Evaluate null-safe literal comparisons against a borrowed native column.
/// Unlike scalar row evaluation this never clones the source TEXT payload.
fn fast_distinct_literal<'a>(
    batch: &'a Batch, predicate: &'a Expr,
) -> Result<Option<(&'a [Value], &'a Value, bool)>> {
    let Expr::IsDistinctFrom { left, right, negated } = predicate else {
        return Ok(None);
    };
    let (name, literal) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(name), Expr::Literal(literal)) |
        (Expr::Literal(literal), Expr::Column(name)) => (name, literal),
        _ => return Ok(None),
    };
    let index = batch.schema().column(name)?;
    Ok(Some((batch.column(index)?, literal, *negated)))
}

/// Return matching row offsets; NULL predicates are not included.
pub fn select_indices(batch: &Batch, predicate: &Expr) -> Result<Vec<usize>> {
    let kind = predicate.output_type(batch.schema())?;
    if kind != DataType::Boolean && !matches!(predicate, Expr::Literal(Value::Null)) {
        return Err(Error::Type("vector filter predicate must be boolean".into()));
    }
    match predicate {
        Expr::Literal(Value::Boolean(true)) => return Ok((0..batch.num_rows()).collect()),
        Expr::Literal(Value::Boolean(false) | Value::Null) => return Ok(Vec::new()),
        _ => {}
    }
    let mut selected = Vec::new();
    if let Some((column, matched, negated, has_null)) =
        fast_boolean_in_list(batch, predicate)? {
        if negated && has_null { return Ok(selected); }
        for (row, value) in column.iter().enumerate() {
            if let Value::Boolean(value) = value {
                let member = matched[usize::from(*value)];
                if if negated { !member } else { member } {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, keys, negated, has_null)) =
        fast_float_in_list(batch, predicate)? {
        if (negated && has_null) || (!negated && keys.is_empty()) {
            return Ok(selected);
        }
        for (row, value) in column.iter().enumerate() {
            if let Value::Float(value) = value {
                let matched = keys.contains(&float_membership_key(*value));
                if if negated { !has_null && !matched } else { matched } {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, keys, negated, has_null)) =
        fast_integer_in_list(batch, predicate)? {
        // NULL needles never satisfy WHERE. An unmatched NOT IN with any
        // NULL choice is also UNKNOWN, not TRUE.
        // Neither case below can admit any row, so avoid probing a large
        // column or building an output selection vector.
        if (negated && has_null) || (!negated && keys.is_empty()) {
            return Ok(selected);
        }
        for (row, value) in column.iter().enumerate() {
            if let Value::Integer(value) = value {
                let matched = keys.contains(value);
                if if negated { !has_null && !matched } else { matched } {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, keys, negated, has_null)) =
        fast_text_in_list(batch, predicate)? {
        if (negated && has_null) || (!negated && keys.is_empty()) {
            return Ok(selected);
        }
        for (row, value) in column.iter().enumerate() {
            if let Value::Text(text) = value {
                let matched = keys.contains(text.as_str());
                if if negated { !has_null && !matched } else { matched } {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, (first_op, first_rhs), (second_op, second_rhs))) =
        fast_integer_conjunction(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Integer(lhs) = value {
                if integer_comparison(first_op, *lhs, first_rhs) == Some(true)
                    && integer_comparison(second_op, *lhs, second_rhs) == Some(true) {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, (first_op, first_rhs), (second_op, second_rhs))) =
        fast_integer_disjunction(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Integer(lhs) = value {
                if integer_comparison(first_op, *lhs, first_rhs) == Some(true)
                    || integer_comparison(second_op, *lhs, second_rhs) == Some(true) {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, op, rhs)) = fast_integer_predicate(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Integer(lhs) = value {
                if integer_comparison(op, *lhs, rhs) == Some(true) {
                    selected.push(row);
                }
            }
        }
    } else if let Some((column, op, rhs)) = fast_float_predicate(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Float(lhs) = value {
                let order = if lhs.is_nan() {
                    if rhs.is_nan() { std::cmp::Ordering::Equal } else { std::cmp::Ordering::Greater }
                } else if rhs.is_nan() {
                    std::cmp::Ordering::Less
                } else {
                    lhs.partial_cmp(&rhs).expect("non-NaN floats are ordered")
                };
                let keep = match op {
                    BinaryOp::Equal => order.is_eq(),
                    BinaryOp::NotEqual => !order.is_eq(),
                    BinaryOp::Less => order.is_lt(),
                    BinaryOp::LessEqual => !order.is_gt(),
                    BinaryOp::Greater => order.is_gt(),
                    BinaryOp::GreaterEqual => !order.is_lt(),
                    _ => false,
                };
                if keep { selected.push(row); }
            }
        }
    } else if let Some((first, second)) =
        fast_boolean_pair(batch, predicate, BinaryOp::And)? {
        for (row, (left, right)) in first.iter().zip(second).enumerate() {
            if matches!(left, Value::Boolean(true))
                && matches!(right, Value::Boolean(true)) {
                selected.push(row);
            }
        }
    } else if let Some((first, second)) =
        fast_boolean_pair(batch, predicate, BinaryOp::Or)? {
        for (row, (left, right)) in first.iter().zip(second).enumerate() {
            if matches!(left, Value::Boolean(true))
                || matches!(right, Value::Boolean(true)) {
                selected.push(row);
            }
        }
    } else if let Some(column) = fast_boolean_column(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if matches!(value, Value::Boolean(true)) { selected.push(row); }
        }
    } else if let Some((column, select_null, nullable)) = fast_null_test(batch, predicate)? {
        if !nullable {
            // Valid native batches cannot contain NULL in a required column.
            return Ok(if select_null { Vec::new() } else { (0..batch.num_rows()).collect() });
        }
        for (row, value) in column.iter().enumerate() {
            if value.is_null() == select_null { selected.push(row); }
        }
    } else if let Expr::Not(inner) = predicate {
        if let Expr::Column(name) = inner.as_ref() {
            let index = batch.schema().column(name)?;
            if batch.schema().field(index)?.data_type == DataType::Boolean {
                for (row, value) in batch.column(index)?.iter().enumerate() {
                    if matches!(value, Value::Boolean(false)) { selected.push(row); }
                }
                return Ok(selected);
            }
        }
        for row in 0..batch.num_rows() {
            if predicate.evaluate(batch, row)?.as_boolean()? == Some(true) { selected.push(row); }
        }
    } else if let Some((column, literal, negated)) =
        fast_distinct_literal(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            let distinct = match (value.is_null(), literal.is_null()) {
                (true, true) => false,
                (true, false) | (false, true) => true,
                (false, false) => value.sql_equal(literal)?.map(|equal| !equal)
                    .unwrap_or_else(|| value != literal),
            };
            if if negated { !distinct } else { distinct } {
                selected.push(row);
            }
        }
    } else if let Some((column, op, rhs)) = fast_text_comparison(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Text(lhs) = value {
                let order = lhs.as_str().cmp(rhs);
                let keep = match op {
                    BinaryOp::Equal => order.is_eq(),
                    BinaryOp::NotEqual => !order.is_eq(),
                    BinaryOp::Less => order.is_lt(),
                    BinaryOp::LessEqual => !order.is_gt(),
                    BinaryOp::Greater => order.is_gt(),
                    BinaryOp::GreaterEqual => !order.is_lt(),
                    _ => unreachable!("matched relational operator"),
                };
                if keep { selected.push(row); }
            }
        }
    } else {
        for row in 0..batch.num_rows() {
            if predicate.evaluate(batch, row)?.as_boolean()? == Some(true) {
                selected.push(row);
            }
        }
    }
    Ok(selected)
}

pub fn filter(batch: &Batch, predicate: &Expr) -> Result<Batch> {
    batch.take(&select_indices(batch, predicate)?)
}

#[cfg(test)]
mod null_in_list_short_circuit_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn null_choices_cannot_select_rows_in_integer_or_text_lists() {
        for (kind, column, literal) in [
            (DataType::Integer, vec![Value::Integer(7), Value::Null,
                Value::Integer(8)], Value::Integer(7)),
            (DataType::Text, vec![Value::Text("seven".into()), Value::Null,
                Value::Text("eight".into())], Value::Text("seven".into())),
        ] {
            let schema = Schema::new(vec![Field::new("k", kind, true)]).unwrap();
            let batch = Batch::from_rows(schema, column.into_iter()
                .map(|value| vec![value]).collect()).unwrap();
            let bad_not_in = Expr::InList {
                needle: Box::new(Expr::Column("k".into())),
                choices: vec![Expr::Literal(literal), Expr::Literal(Value::Null)],
                negated: true,
            };
            let only_null = Expr::InList {
                needle: Box::new(Expr::Column("k".into())),
                choices: vec![Expr::Literal(Value::Null)],
                negated: false,
            };
            for predicate in [bad_not_in, only_null] {
                assert_eq!(select_indices(&batch, &predicate).unwrap(), Vec::<usize>::new());
                for row in 0..batch.num_rows() {
                    assert_ne!(predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap(),
                        Some(true));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn empty_batches_reject_invalid_predicates() {
        let batch = Batch::empty(Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap());
        assert!(select_indices(&batch, &Expr::Column("n".into())).is_err());
        assert!(select_indices(&batch, &Expr::Column("missing".into())).is_err());
        assert!(select_indices(&batch, &Expr::Literal(Value::Null)).unwrap().is_empty());
    }

    #[test]
    fn text_equality_fastpath_preserves_unicode_null_and_duplicate_values() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("city", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("東京".into())], vec![Value::Null],
                vec![Value::Text("Paris".into())], vec![Value::Text("東京".into())]],
        ).unwrap();
        for op in [BinaryOp::Equal, BinaryOp::NotEqual] {
            let predicate = Expr::binary(Expr::Column("city".into()), op,
                Expr::Literal(Value::Text("東京".into())));
            let expected = (0..batch.num_rows()).filter(|&row| {
                predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
            }).collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
        }
    }

    #[test]
    fn boolean_column_fastpath_skips_false_and_null_rows() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("enabled", DataType::Boolean, true)]).unwrap(),
            vec![vec![Value::Boolean(true)], vec![Value::Null],
                vec![Value::Boolean(false)], vec![Value::Boolean(true)]],
        ).unwrap();
        let predicate = Expr::Column("enabled".into());
        assert_eq!(select_indices(&batch, &predicate).unwrap(), vec![0, 3]);
    }

    #[test]
    fn float_fastpath_matches_scalar_semantics_including_nan_and_null() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Float, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Float(f64::NAN)],
                vec![Value::Float(-0.0)], vec![Value::Float(1.25)],
                vec![Value::Float(3.5)]],
        ).unwrap();
        for op in [BinaryOp::Equal, BinaryOp::NotEqual, BinaryOp::Less,
            BinaryOp::LessEqual, BinaryOp::Greater, BinaryOp::GreaterEqual] {
            let predicate = Expr::binary(Expr::Column("n".into()), op,
                Expr::Literal(Value::Float(1.25)));
            let expected = (0..batch.num_rows()).filter(|&row| {
                predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
            }).collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
        }
    }

    #[test]
    fn fast_path_matches_general_expression_semantics() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![
                vec![Value::Null], vec![Value::Integer(-2)],
                vec![Value::Integer(5)], vec![Value::Integer(7)],
            ]
        ).unwrap();
        for op in [
            BinaryOp::Equal, BinaryOp::NotEqual, BinaryOp::Less,
            BinaryOp::LessEqual, BinaryOp::Greater, BinaryOp::GreaterEqual,
        ] {
            let predicate = Expr::binary(
                Expr::Column("n".into()), op, Expr::Literal(Value::Integer(5)),
            );
            let expected = (0..batch.num_rows())
                .filter(|&row| predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true))
                .collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
        }
    }

    #[test]
    fn fallback_preserves_three_valued_logic() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(2)]],
        ).unwrap();
        let predicate = Expr::IsNull(Box::new(Expr::Column("n".into())));
        assert_eq!(select_indices(&batch, &predicate).unwrap(), vec![0]);
    }
}

#[cfg(test)]
mod constant_left_filter_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn reversed_comparisons_match_scalar_sql_evaluation() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(-1)],
                 vec![Value::Integer(5)], vec![Value::Integer(9)]],
        ).unwrap();
        for op in [BinaryOp::Equal, BinaryOp::NotEqual, BinaryOp::Less,
                   BinaryOp::LessEqual, BinaryOp::Greater, BinaryOp::GreaterEqual] {
            let pred = Expr::binary(Expr::Literal(Value::Integer(5)),
                op, Expr::Column("n".into()));
            let scalar = (0..batch.num_rows())
                .filter(|&row| pred.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true))
                .collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &pred).unwrap(), scalar);
        }
    }
}

#[cfg(test)]
mod reversed_text_comparison_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn literal_left_comparisons_match_scalar_on_nulls_and_unicode() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("city", DataType::Text, true)]).unwrap(),
            vec![
                vec![Value::Null],
                vec![Value::Text("東京".into())],
                vec![Value::Text("Paris".into())],
                vec![Value::Text("東京".into())],
            ],
        ).unwrap();
        for op in [BinaryOp::Equal, BinaryOp::NotEqual] {
            let predicate = Expr::binary(
                Expr::Literal(Value::Text("東京".into())),
                op, Expr::Column("city".into()),
            );
            let expected = (0..input.num_rows()).filter(|&row| {
                predicate.evaluate(&input, row).unwrap().as_boolean().unwrap() == Some(true)
            }).collect::<Vec<_>>();
            assert_eq!(select_indices(&input, &predicate).unwrap(), expected);
        }
    }
}

#[cfg(test)]
mod reversed_float_fast_path_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn constant_left_float_comparisons_match_scalar_nan_semantics() {
        let batch = Batch::from_rows(Schema::new(vec![Field::new("x", DataType::Float, true)]).unwrap(), vec![
            vec![Value::Null], vec![Value::Float(-0.0)],
            vec![Value::Float(2.5)], vec![Value::Float(f64::NAN)],
        ]).unwrap();
        for rhs in [Value::Float(1.5), Value::Float(f64::NAN)] {
            for op in [BinaryOp::Equal, BinaryOp::NotEqual, BinaryOp::Less, BinaryOp::LessEqual,
                       BinaryOp::Greater, BinaryOp::GreaterEqual] {
                let expr = Expr::binary(Expr::Literal(rhs.clone()), op, Expr::Column("x".into()));
                let expected = (0..batch.num_rows()).filter(|&row|
                    expr.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
                ).collect::<Vec<_>>();
                assert_eq!(select_indices(&batch, &expr).unwrap(), expected);
            }
        }
    }
}

#[cfg(test)]
mod null_column_fast_filter_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn null_and_not_null_selection_match_scalar_logic() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap(),
            vec![
                vec![Value::Null], vec![Value::Text("é".into())],
                vec![Value::Text("".into())], vec![Value::Null],
            ],
        ).unwrap();
        let null_check = Expr::IsNull(Box::new(Expr::Column("payload".into())));
        let not_null_check = Expr::Not(Box::new(null_check.clone()));
        assert_eq!(select_indices(&input, &null_check).unwrap(), vec![0, 3]);
        assert_eq!(select_indices(&input, &not_null_check).unwrap(), vec![1, 2]);
        for predicate in [&null_check, &not_null_check] {
            let expected = (0..input.num_rows()).filter(|&row|
                predicate.evaluate(&input, row).unwrap().as_boolean().unwrap() == Some(true)
            ).collect::<Vec<_>>();
            assert_eq!(select_indices(&input, predicate).unwrap(), expected);
        }
        assert!(select_indices(&input,
            &Expr::IsNull(Box::new(Expr::Column("missing".into())))).is_err());
    }
}

#[cfg(test)]
mod native_negated_boolean_scan_tests {
    use super::*;
    use crate::schema::{Schema, Field, DataType};

    #[test]
    fn not_boolean_column_excludes_nulls_without_reordering() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("enabled", DataType::Boolean, true)]).unwrap(),
            vec![vec![Value::Boolean(true)], vec![Value::Null],
                 vec![Value::Boolean(false)], vec![Value::Boolean(false)]],
        ).unwrap();
        assert_eq!(select_indices(&input,
            &Expr::Not(Box::new(Expr::Column("enabled".into())))
        ).unwrap(), [2, 3]);
    }
}

#[cfg(test)]
mod nonnullable_null_test_fast_path_tests {
    use super::*;
    use crate::{schema::{Schema, Field, DataType}, scalar::Value};
    #[test]
    fn required_columns_skip_is_null_value_scans() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, (0..1000).map(|n|
            vec![Value::Integer(n)]).collect()).unwrap();
        let is_null = Expr::IsNull(Box::new(Expr::Column("n".into())));
        let not_null = Expr::Not(Box::new(is_null.clone()));
        assert!(select_indices(&input, &is_null).unwrap().is_empty());
        assert_eq!(select_indices(&input, &not_null).unwrap(),
            (0..input.num_rows()).collect::<Vec<_>>());
    }
}

#[cfg(test)]
mod fused_integer_predicate_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn compound_ranges_match_scalar_results_with_nulls_and_reversed_operands() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("m", DataType::Integer, false),
        ]).unwrap(), (0..512).map(|row| vec![
            if row % 13 == 0 { Value::Null } else { Value::Integer(row - 120) },
            Value::Integer(row % 19),
        ]).collect()).unwrap();
        let column = || Expr::Column("n".into());
        for (left_op, right_op) in [
            (BinaryOp::GreaterEqual, BinaryOp::Less),
            (BinaryOp::Greater, BinaryOp::LessEqual),
            (BinaryOp::Equal, BinaryOp::NotEqual),
        ] {
            let predicate = Expr::binary(
                Expr::binary(column(), left_op, Expr::Literal(Value::Integer(0))),
                BinaryOp::And,
                Expr::binary(Expr::Literal(Value::Integer(80)), right_op, column()),
            );
            assert!(fast_integer_conjunction(&batch, &predicate).unwrap().is_some());
            let expected = (0..batch.num_rows()).filter(|&row|
                predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
            ).collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
        }
    }

    #[test]
    fn distinct_integer_columns_and_noninteger_predicates_use_fallback() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("x", DataType::Integer, true),
            Field::new("y", DataType::Integer, false),
        ]).unwrap(), vec![
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(5)],
            vec![Value::Integer(7), Value::Integer(8)],
        ]).unwrap();
        let compare = |name: &str, op, value| Expr::binary(
            Expr::Column(name.into()), op, Expr::Literal(Value::Integer(value))
        );
        let predicate = Expr::binary(
            compare("x", BinaryOp::Greater, 1), BinaryOp::And,
            compare("y", BinaryOp::Less, 6)
        );
        assert!(fast_integer_conjunction(&batch, &predicate).unwrap().is_none());
        let expected = (0..batch.num_rows()).filter(|&row|
            predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
        ).collect::<Vec<_>>();
        assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
    }
}

#[cfg(test)]
mod integer_disjunction_vector_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn fused_or_matches_sql_three_valued_logic_and_reversed_predicates() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Integer, true),
            Field::new("y", DataType::Integer, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Integer(-5), Value::Integer(2)],
            vec![Value::Integer(0), Value::Integer(2)],
            vec![Value::Integer(3), Value::Null],
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Integer(7), Value::Integer(2)],
            vec![Value::Integer(9), Value::Null],
        ]).unwrap();
        let low = Expr::binary(Expr::Column("x".into()), BinaryOp::Less,
            Expr::Literal(Value::Integer(0)));
        let high = Expr::binary(Expr::Literal(Value::Integer(7)), BinaryOp::LessEqual,
            Expr::Column("x".into()));
        let predicate = Expr::binary(low.clone(), BinaryOp::Or, high.clone());
        assert!(fast_integer_disjunction(&batch, &predicate).unwrap().is_some());
        assert_eq!(select_indices(&batch, &predicate).unwrap(), vec![0, 4, 5]);
        let expected = (0..batch.num_rows()).filter(|&i|
            predicate.evaluate(&batch, i).unwrap().as_boolean().unwrap() == Some(true)
        ).collect::<Vec<_>>();
        assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);

        let other = Expr::binary(Expr::Column("y".into()), BinaryOp::Equal,
            Expr::Literal(Value::Integer(2)));
        let mixed = Expr::binary(low, BinaryOp::Or, other);
        assert!(fast_integer_disjunction(&batch, &mixed).unwrap().is_none());
        let expected = (0..batch.num_rows()).filter(|&i|
            mixed.evaluate(&batch, i).unwrap().as_boolean().unwrap() == Some(true)
        ).collect::<Vec<_>>();
        assert_eq!(select_indices(&batch, &mixed).unwrap(), expected);
    }

    #[test]
    fn null_only_disjunction_produces_no_matches() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Null]]).unwrap();
        let predicate = Expr::binary(
            Expr::binary(Expr::Column("n".into()), BinaryOp::Equal,
                Expr::Literal(Value::Integer(3))),
            BinaryOp::Or,
            Expr::binary(Expr::Column("n".into()), BinaryOp::Greater,
                Expr::Literal(Value::Integer(5))),
        );
        assert_eq!(select_indices(&batch, &predicate).unwrap(), Vec::<usize>::new());
    }
}

#[cfg(test)]
mod hashed_integer_in_list_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    fn assert_matches_scalar(batch: &Batch, predicate: Expr) {
        let expected = (0..batch.num_rows())
            .filter(|&row| predicate.evaluate(batch, row).unwrap().as_boolean().unwrap() == Some(true))
            .collect::<Vec<_>>();
        assert_eq!(select_indices(batch, &predicate).unwrap(), expected);
    }

    #[test]
    fn large_in_lists_match_scalar_sql_for_nulls_duplicates_and_not_in() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema,
            (0..500).map(|n| vec![if n % 13 == 0 {
                Value::Null
            } else { Value::Integer(n % 257) }]).collect()
        ).unwrap();
        let choices = (0..500).map(|n| Expr::Literal(Value::Integer(n % 131)))
            .chain([Expr::Literal(Value::Integer(5)), Expr::Literal(Value::Null)])
            .collect::<Vec<_>>();
        for negated in [false, true] {
            assert_matches_scalar(&batch, Expr::InList {
                needle: Box::new(Expr::Column("id".into())),
                choices: choices.clone(), negated
            });
        }
        assert_matches_scalar(&batch, Expr::InList {
            needle: Box::new(Expr::Column("id".into())),
            choices: vec![], negated: true
        });
        assert_matches_scalar(&batch, Expr::InList {
            needle: Box::new(Expr::Column("id".into())),
            choices: vec![
                Expr::Literal(Value::Integer(2)),
                Expr::Literal(Value::Float(3.0)),
            ], negated: false
        });
    }
}

#[cfg(test)]
mod text_in_list_fast_path_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn matches_scalar_sql_for_nullable_unicode_long_and_duplicate_literals() {
        let large = "é🙂東京".repeat(1024);
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("city", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Text(large.clone())],
            vec![Value::Text("Berlin".into())],
            vec![Value::Null],
            vec![Value::Text("東京".into())],
            vec![Value::Text(large.clone())],
            vec![Value::Text("".into())],
        ]).unwrap();
        for negated in [false, true] {
            for null_choice in [false, true] {
                let mut choices = vec![
                    Expr::Literal(Value::Text(large.clone())),
                    Expr::Literal(Value::Text("東京".into())),
                    Expr::Literal(Value::Text(large.clone())),
                ];
                if null_choice { choices.push(Expr::Literal(Value::Null)); }
                let predicate = Expr::InList {
                    needle: Box::new(Expr::Column("city".into())),
                    choices, negated,
                };
                let expected = (0..batch.num_rows()).filter(|&row| {
                    predicate.evaluate(&batch, row).unwrap().as_boolean().unwrap() == Some(true)
                }).collect::<Vec<_>>();
                assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
            }
        }
    }

    #[test]
    fn falls_back_for_computed_members_and_rejects_incorrect_column_names() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("text", DataType::Text, false),
        ]).unwrap(), vec![vec![Value::Text("abc".into())]]).unwrap();
        let computed = Expr::InList {
            needle: Box::new(Expr::Column("text".into())),
            choices: vec![Expr::Column("text".into())], negated: false,
        };
        assert_eq!(select_indices(&batch, &computed).unwrap(), vec![0]);
        let missing = Expr::InList {
            needle: Box::new(Expr::Column("missing".into())),
            choices: vec![Expr::Literal(Value::Text("abc".into()))], negated: false,
        };
        assert!(select_indices(&batch, &missing).is_err());
    }
}

#[cfg(test)]
mod fused_boolean_columns_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn boolean_pair_matches_scalar_three_valued_logic() {
        let choices = [Value::Boolean(false), Value::Boolean(true), Value::Null];
        let rows = choices.iter().flat_map(|first| choices.iter().map(move |second|
            vec![first.clone(), second.clone()])).collect::<Vec<_>>();
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]).unwrap(), rows).unwrap();
        for op in [BinaryOp::And, BinaryOp::Or] {
            let predicate = Expr::binary(
                Expr::Column("a".into()), op, Expr::Column("b".into()));
            assert!(fast_boolean_pair(&batch, &predicate, op).unwrap().is_some());
            let expected = (0..batch.num_rows())
                .filter(|&row| predicate.evaluate(&batch, row).unwrap()
                    .as_boolean().unwrap() == Some(true))
                .collect::<Vec<_>>();
            assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
        }
        let same = Expr::binary(
            Expr::Column("a".into()), BinaryOp::Or, Expr::Column("a".into()));
        assert_eq!(select_indices(&batch, &same).unwrap(), vec![3, 4, 5]);
    }

    #[test]
    fn computed_boolean_expressions_keep_scalar_fallback() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]).unwrap(), vec![
            vec![Value::Boolean(true), Value::Boolean(false)],
            vec![Value::Boolean(false), Value::Boolean(true)],
            vec![Value::Null, Value::Boolean(true)],
        ]).unwrap();
        let predicate = Expr::binary(
            Expr::Not(Box::new(Expr::Column("a".into()))),
            BinaryOp::Or, Expr::Column("b".into()));
        assert!(fast_boolean_pair(&batch, &predicate, BinaryOp::Or).unwrap().is_none());
        let expected = (0..batch.num_rows())
            .filter(|&row| predicate.evaluate(&batch, row).unwrap()
                .as_boolean().unwrap() == Some(true))
            .collect::<Vec<_>>();
        assert_eq!(select_indices(&batch, &predicate).unwrap(), expected);
    }
}

#[cfg(test)]
mod float_in_membership_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn float_sets_match_sql_for_nan_signed_zero_nulls_and_infinities() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("v", DataType::Float, true)]).unwrap(),
            [Value::Null, Value::Float(f64::NAN), Value::Float(-f64::NAN),
             Value::Float(-0.0), Value::Float(0.0), Value::Float(-0.25),
             Value::Float(1.5), Value::Float(f64::INFINITY),
             Value::Float(f64::NEG_INFINITY)]
                .into_iter().map(|value| vec![value]).collect(),
        ).unwrap();
        for choices in [
            vec![Value::Float(f64::NAN), Value::Float(0.0)],
            vec![Value::Float(-0.0), Value::Float(1.5), Value::Null],
            vec![Value::Null],
            vec![],
            vec![Value::Float(f64::NEG_INFINITY), Value::Float(f64::INFINITY)],
        ] {
            for negated in [true, false] {
                let predicate = Expr::InList {
                    needle: Box::new(Expr::Column("v".into())),
                    choices: choices.iter().cloned().map(Expr::Literal).collect(),
                    negated,
                };
                let expected = (0..batch.num_rows()).filter(|&row|
                    predicate.evaluate(&batch, row).unwrap()
                        .as_boolean().unwrap() == Some(true)
                ).collect::<Vec<_>>();
                assert_eq!(select_indices(&batch, &predicate).unwrap(), expected,
                    "choices={choices:?}, negated={negated}");
            }
        }
    }

    #[test]
    fn mixed_numeric_lists_retain_exact_scalar_comparisons() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("v", DataType::Float, false)]).unwrap(),
            vec![vec![Value::Float(9_007_199_254_740_992.0)]],
        ).unwrap();
        let predicate = Expr::InList {
            needle: Box::new(Expr::Column("v".into())),
            choices: vec![Expr::Literal(Value::Integer(9_007_199_254_740_993))],
            negated: false,
        };
        assert!(select_indices(&batch, &predicate).unwrap().is_empty());
    }
}

#[cfg(test)]
mod borrowed_text_ordering_vector_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn all_text_orderings_match_scalar_for_both_operand_orders() {
        let large = "漢字é🙂".repeat(2048);
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap(),
            vec![
                Value::Null, Value::Text("".into()),
                Value::Text("é".into()), Value::Text("e".into()),
                Value::Text("東京".into()), Value::Text("🙂".into()),
                Value::Text(large.clone()), Value::Text(large.clone()),
            ].into_iter().map(|value| vec![value]).collect(),
        ).unwrap();
        for literal in ["", "é", "e", "東京", "🙂", large.as_str(), "missing"] {
            for op in [
                BinaryOp::Equal, BinaryOp::NotEqual,
                BinaryOp::Less, BinaryOp::LessEqual,
                BinaryOp::Greater, BinaryOp::GreaterEqual,
            ] {
                for literal_left in [false, true] {
                    let (left, right) = if literal_left {
                        (Expr::Literal(Value::Text(literal.into())),
                            Expr::Column("text".into()))
                    } else {
                        (Expr::Column("text".into()),
                            Expr::Literal(Value::Text(literal.into())))
                    };
                    let predicate = Expr::binary(left, op, right);
                    let expected = (0..batch.num_rows()).filter(|&row|
                        predicate.evaluate(&batch, row).unwrap()
                            .as_boolean().unwrap() == Some(true)
                    ).collect::<Vec<_>>();
                    assert_eq!(select_indices(&batch, &predicate).unwrap(), expected,
                        "operator={op:?}, literal_left={literal_left}");
                }
            }
        }
    }
}
#[cfg(test)]
mod boolean_in_list_vector_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn all_boolean_choice_combinations_match_scalar_sql() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("flag", DataType::Boolean, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Boolean(true)],
                 vec![Value::Boolean(false)], vec![Value::Boolean(true)]],
        ).unwrap();
        for choices in [
            vec![], vec![Value::Null],
            vec![Value::Boolean(false)],
            vec![Value::Boolean(true), Value::Boolean(true)],
            vec![Value::Boolean(false), Value::Boolean(true)],
            vec![Value::Boolean(true), Value::Null],
        ] {
            for negated in [false, true] {
                let predicate = Expr::InList {
                    needle: Box::new(Expr::Column("flag".into())),
                    choices: choices.iter().cloned().map(Expr::Literal).collect(),
                    negated,
                };
                let expected = (0..input.num_rows()).filter(|&row|
                    predicate.evaluate(&input, row).unwrap()
                        .as_boolean().unwrap() == Some(true)
                ).collect::<Vec<_>>();
                assert_eq!(select_indices(&input, &predicate).unwrap(),
                    expected, "choices={choices:?}, negated={negated}");
            }
        }
    }
}
#[cfg(test)]
mod literal_distinct_vector_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    fn check(kind: DataType, values: Vec<Value>, literals: Vec<Value>) {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("key", kind, true)]).unwrap(),
            values.into_iter().map(|value| vec![value]).collect(),
        ).unwrap();
        for literal in literals {
            for negated in [true, false] {
                for reversed in [true, false] {
                    let (left, right) = if reversed {
                        (Expr::Literal(literal.clone()), Expr::Column("key".into()))
                    } else {
                        (Expr::Column("key".into()), Expr::Literal(literal.clone()))
                    };
                    let predicate = Expr::IsDistinctFrom {
                        left: Box::new(left), right: Box::new(right), negated,
                    };
                    let expected = (0..batch.num_rows()).filter(|&row|
                        predicate.evaluate(&batch, row).unwrap()
                            .as_boolean().unwrap() == Some(true)
                    ).collect::<Vec<_>>();
                    assert_eq!(select_indices(&batch, &predicate).unwrap(), expected,
                        "{predicate:?}");
                }
            }
        }
    }

    #[test]
    fn null_safe_text_comparisons_match_scalar_with_unicode_and_long_strings() {
        let large = "東京🙂".repeat(4096);
        check(DataType::Text, vec![
            Value::Null, Value::Text("".into()), Value::Text(large.clone()),
            Value::Text(large.clone()), Value::Text("é".into()),
        ], vec![Value::Null, Value::Text(large), Value::Text("missing".into())]);
    }

    #[test]
    fn float_nan_and_zero_and_cross_type_numeric_comparisons_match_scalar() {
        check(DataType::Float, vec![
            Value::Null, Value::Float(f64::NAN), Value::Float(-f64::NAN),
            Value::Float(-0.0), Value::Float(0.0),
            Value::Float(1.25), Value::Float(f64::INFINITY),
            Value::Float(9_007_199_254_740_992.0),
        ], vec![Value::Null, Value::Float(f64::NAN), Value::Float(0.0),
                Value::Integer(9_007_199_254_740_993)]);
    }

    #[test]
    fn integer_and_boolean_domains_match_scalar_for_null_safe_comparisons() {
        check(DataType::Integer, vec![
            Value::Null, Value::Integer(-3), Value::Integer(0), Value::Integer(5),
        ], vec![Value::Null, Value::Integer(0), Value::Integer(999)]);
        check(DataType::Boolean, vec![
            Value::Null, Value::Boolean(true), Value::Boolean(false),
        ], vec![Value::Null, Value::Boolean(true)]);
    }
}

