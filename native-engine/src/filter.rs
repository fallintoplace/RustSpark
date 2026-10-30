use crate::{batch::Batch, expr::Expr, scalar::Value, schema::DataType, Error, Result};

/// WHERE keeps only rows for which the predicate evaluates to SQL TRUE.
pub fn filter(batch: &Batch, predicate: &Expr) -> Result<Batch> {
    if matches!(predicate, Expr::Literal(Value::Boolean(true))) {
        return Ok(batch.clone());
    }
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) {
        return Ok(Batch::empty(batch.schema().clone()));
    }
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
    let kind = predicate.output_type(batch.schema())?;
    if kind != DataType::Boolean && !matches!(predicate, Expr::Literal(Value::Null)) {
        return Err(Error::Type("WHERE predicate must be boolean".into()));
    }
    // Validate the predicate first, then bypass row-wise evaluation when
    // SQL three-valued semantics determine every row's outcome.
    if matches!(predicate, Expr::Literal(Value::Boolean(true))) {
        return batch.slice_rows(0, max_rows.min(batch.num_rows()));
    }
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) {
        return batch.slice_rows(0, 0);
    }
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


#[cfg(test)]
mod vectorized_filter_integration_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn default_filter_uses_sql_null_semantics_for_integer_comparisons() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(3)], vec![Value::Integer(9)]],
        ).unwrap();
        let expr = Expr::binary(Expr::Column("n".into()),
            crate::expr::BinaryOp::Greater, Expr::Literal(Value::Integer(3)));
        assert_eq!(filter(&input, &expr).unwrap().rows().unwrap(),
            vec![vec![Value::Integer(9)]]);
    }
}

#[cfg(test)]
mod consolidated_filter_predicate_preflight_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};
    #[test]
    fn invalid_predicates_fail_on_empty_inputs_and_zero_limits() {
        let batch = Batch::empty(Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap());
        for bad in [Expr::Column("n".into()), Expr::Column("unknown".into())] {
            assert!(filter(&batch, &bad).is_err());
            assert!(filter_first(&batch, &bad, 0).is_err());
        }
        assert_eq!(filter_first(&batch, &Expr::Literal(Value::Null), 0).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod literal_bounded_filter_perf_tests {
    use super::*;
    use crate::schema::Schema;
    #[test]
    fn true_false_and_null_predicates_skip_unneeded_scan_work() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "n", crate::schema::DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, (0..5)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let true_rows = filter_first(&batch, &Expr::Literal(Value::Boolean(true)), 2).unwrap();
        assert_eq!(true_rows.num_rows(), 2);
        assert_eq!(true_rows.row(1).unwrap(), vec![Value::Integer(1)]);
        for value in [Value::Boolean(false), Value::Null] {
            assert_eq!(filter_first(&batch, &Expr::Literal(value), 2).unwrap().num_rows(), 0);
        }
    }
}

#[cfg(test)]
mod constant_filter_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn false_and_unknown_filters_produce_empty_typed_results() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Text, true),
        ]).unwrap(), vec![vec![Value::Null], vec![Value::Text("x".into())]]).unwrap();
        for predicate in [Expr::Literal(Value::Boolean(false)), Expr::Literal(Value::Null)] {
            let output = filter(&input, &predicate).unwrap();
            assert_eq!(output.num_rows(), 0);
            assert_eq!(output.schema(), input.schema());
        }
        assert_eq!(filter(&input, &Expr::Literal(Value::Boolean(true))).unwrap(), input);
    }
}
