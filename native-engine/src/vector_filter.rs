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

fn fast_boolean_column<'a>(batch: &'a Batch, predicate: &Expr) -> Result<Option<&'a [Value]>> {
    let Expr::Column(name) = predicate else { return Ok(None); };
    let index = batch.schema().column(name)?;
    if batch.schema().field(index)?.data_type == DataType::Boolean {
        Ok(Some(batch.column(index)?))
    } else {
        Ok(None)
    }
}

fn fast_float_predicate<'a>(
    batch: &'a Batch,
    predicate: &'a Expr,
) -> Result<Option<(&'a [Value], BinaryOp, f64)>> {
    if let Expr::Binary { left, op, right } = predicate {
        if let (Expr::Column(column), Expr::Literal(Value::Float(value))) =
            (left.as_ref(), right.as_ref()) {
            if integer_comparison(*op, 0, 0).is_some() {
                let index = batch.schema().column(column)?;
                if batch.schema().field(index)?.data_type == DataType::Float {
                    return Ok(Some((batch.column(index)?, *op, *value)));
                }
            }
        }
    }
    Ok(None)
}

fn fast_integer_predicate<'a>(
    batch: &'a Batch,
    predicate: &'a Expr,
) -> Result<Option<(&'a [Value], BinaryOp, i64)>> {
    if let Expr::Binary { left, op, right } = predicate {
        if let (Expr::Column(column), Expr::Literal(Value::Integer(value))) =
            (left.as_ref(), right.as_ref()) {
            if integer_comparison(*op, 0, 0).is_some() {
                let index = batch.schema().column(column)?;
                if batch.schema().field(index)?.data_type == crate::schema::DataType::Integer {
                    return Ok(Some((batch.column(index)?, *op, *value)));
                }
            }
        }
    }
    Ok(None)
}

/// Return matching row offsets; NULL predicates are not included.
pub fn select_indices(batch: &Batch, predicate: &Expr) -> Result<Vec<usize>> {
    let kind = predicate.output_type(batch.schema())?;
    if kind != DataType::Boolean && !matches!(predicate, Expr::Literal(Value::Null)) {
        return Err(Error::Type("vector filter predicate must be boolean".into()));
    }
    let mut selected = Vec::new();
    if let Some((column, op, rhs)) = fast_integer_predicate(batch, predicate)? {
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
                if let Some(order) = lhs.partial_cmp(&rhs) {
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
        }
    } else if let Some(column) = fast_boolean_column(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if matches!(value, Value::Boolean(true)) { selected.push(row); }
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
