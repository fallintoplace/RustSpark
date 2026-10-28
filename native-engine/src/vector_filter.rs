//! Typed filter fast path over native column buffers with SQL-null semantics.
use crate::{batch::Batch, expr::{BinaryOp, Expr}, scalar::Value, Result};

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
    let mut selected = Vec::new();
    if let Some((column, op, rhs)) = fast_integer_predicate(batch, predicate)? {
        for (row, value) in column.iter().enumerate() {
            if let Value::Integer(lhs) = value {
                if integer_comparison(op, *lhs, rhs) == Some(true) {
                    selected.push(row);
                }
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
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

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
