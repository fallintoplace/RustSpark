use crate::{batch::Batch, expr::Expr, Result};

/// WHERE keeps only rows for which the predicate evaluates to SQL TRUE.
pub fn filter(batch: &Batch, predicate: &Expr) -> Result<Batch> {
    batch.take(&crate::vector_filter::select_indices(batch, predicate)?)
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

 
/// Return at most max_rows matching rows, retaining their source order.
pub fn filter_first(batch: &Batch, predicate: &Expr, max_rows: usize) -> Result<Batch> {
    let mut keep = Vec::with_capacity(max_rows.min(batch.num_rows()));
    for row in 0..batch.num_rows() {
        if keep.len() == max_rows { break; }
        if predicate.evaluate(batch, row)?.as_boolean()? == Some(true) {
            keep.push(row);
        }
    }
    batch.take(&keep)
}

#[cfg(test)]
mod bounded_filter_feature_tests {
    use super::*;
    use crate::{expr::BinaryOp, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn returns_only_first_matching_rows_and_ignores_unknown() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, true)
        ]).unwrap(), vec![
            vec![Value::Null], vec![Value::Integer(1)],
            vec![Value::Integer(2)], vec![Value::Integer(3)]
        ]).unwrap();
        let pred = Expr::binary(Expr::Column("id".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(0)));
        assert_eq!(filter_first(&input, &pred, 2).unwrap().rows().unwrap(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]);
        assert_eq!(filter_first(&input, &pred, 0).unwrap().num_rows(), 0);
    }
}

