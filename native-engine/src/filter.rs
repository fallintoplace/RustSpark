use crate::{batch::Batch, expr::Expr, Result};

/// WHERE keeps only rows for which the predicate evaluates to SQL TRUE.
pub fn filter(batch: &Batch, predicate: &Expr) -> Result<Batch> {
    let mut keep = Vec::new();
    for row in 0..batch.num_rows() {
        if predicate.evaluate(batch, row)?.as_boolean()? == Some(true) {
            keep.push(row);
        }
    }
    batch.take(&keep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::BinaryOp,
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn unknown_predicates_do_not_pass_where() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![
                vec![Value::Null],
                vec![Value::Integer(1)],
                vec![Value::Integer(2)],
            ],
        )
        .unwrap();
        let predicate = Expr::binary(
            Expr::Column("n".into()),
            BinaryOp::Greater,
            Expr::Literal(Value::Integer(1)),
        );
        let result = filter(&batch, &predicate).unwrap();
        assert_eq!(result.rows().unwrap(), vec![vec![Value::Integer(2)]]);
    }
}
