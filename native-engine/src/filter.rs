use crate::{batch::Batch, expr::Expr, scalar::Value, schema::DataType, Error, Result};

/// WHERE keeps only rows for which the predicate evaluates to SQL TRUE.
pub fn filter(batch: &Batch, predicate: &Expr) -> Result<Batch> {
    predicate.validate_budget()?;
    if matches!(predicate, Expr::Literal(Value::Boolean(true))) {
        return Ok(batch.clone());
    }
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) {
        // The result is known without scanning any source columns.
        return Ok(Batch::empty(batch.schema().clone()));
    }
    let indices = crate::vector_filter::select_indices(batch, predicate)?;
    if indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    if indices.is_empty() {
        return Ok(Batch::empty(batch.schema().clone()));
    }
    batch.take(&indices)
}

/// Filter an owned native batch. An always-true predicate can reuse all columns.
pub fn filter_owned(batch: Batch, predicate: &Expr) -> Result<Batch> {
    predicate.validate_budget()?;
    if matches!(predicate, Expr::Literal(Value::Boolean(true))) {
        return Ok(batch);
    }
    if matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) {
        return Ok(Batch::empty(batch.schema().clone()));
    }
    let indices = crate::vector_filter::select_indices(&batch, predicate)?;
    if indices.len() == batch.num_rows() {
        return Ok(batch);
    }
    if indices.is_empty() {
        return Ok(Batch::empty(batch.schema().clone()));
    }
    batch.take_sorted_owned(&indices)
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
    predicate.validate_budget()?;
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
    // Keep type validation above this shortcut, including when no rows are requested.
    if max_rows == 0 { return batch.slice_rows(0, 0); }
    let direct_column = match predicate {
        Expr::Column(name) => Some((name.as_str(), true)),
        Expr::Not(inner) => match inner.as_ref() {
            Expr::Column(name) => Some((name.as_str(), false)),
            _ => None,
        },
        _ => None,
    };
    if let Some((name, wanted)) = direct_column {
        let index = batch.schema().column(name)?;
        let column = batch.column(index)?;
        let mut keep = Vec::with_capacity(max_rows.min(batch.num_rows()));
        for (row, value) in column.iter().enumerate() {
            if matches!(value, Value::Boolean(actual) if *actual == wanted) {
                keep.push(row);
                if keep.len() == max_rows { break; }
            }
        }
        return batch.take(&keep);
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

/// Select only the first matches from an owned batch. Matched TEXT payloads
/// move into the result, and expressions after the requested limit are not run.
pub fn filter_first_owned(batch: Batch, predicate: &Expr, max_rows: usize) -> Result<Batch> {
    predicate.validate_budget()?;
    let kind = predicate.output_type(batch.schema())?;
    if kind != DataType::Boolean && !matches!(predicate, Expr::Literal(Value::Null)) {
        return Err(Error::Type("WHERE predicate must be boolean".into()));
    }
    if max_rows == 0 ||
        matches!(predicate, Expr::Literal(Value::Boolean(false) | Value::Null)) {
        return Ok(Batch::empty(batch.schema().clone()));
    }
    if matches!(predicate, Expr::Literal(Value::Boolean(true))) {
        let end = max_rows.min(batch.num_rows());
        return batch.slice_rows_owned(0, end);
    }
    let mut keep = Vec::with_capacity(max_rows.min(batch.num_rows()));
    let direct = match predicate {
        Expr::Column(name) => Some((name.as_str(), true)),
        Expr::Not(inner) => match inner.as_ref() {
            Expr::Column(name) => Some((name.as_str(), false)),
            _ => None,
        },
        _ => None,
    };
    if let Some((name, wanted)) = direct {
        let values = batch.column(batch.schema().column(name)?)?;
        for (row, value) in values.iter().enumerate() {
            if matches!(value, Value::Boolean(actual) if *actual == wanted) {
                keep.push(row);
                if keep.len() == max_rows { break; }
            }
        }
    } else {
        for row in 0..batch.num_rows() {
            if predicate.evaluate(&batch, row)?.as_boolean()? == Some(true) {
                keep.push(row);
                if keep.len() == max_rows { break; }
            }
        }
    }
    if keep.len() == batch.num_rows() {
        return Ok(batch);
    }
    batch.take_sorted_owned(&keep)
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
mod owned_filter_operator_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn ownership_fast_path_preserves_nullable_payloads() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("payload", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Text("large".repeat(1024))], vec![Value::Null],
        ]).unwrap();
        assert_eq!(filter_owned(batch.clone(), &Expr::Literal(Value::Boolean(true))).unwrap(), batch);
        assert_eq!(filter_owned(batch.clone(), &Expr::Literal(Value::Boolean(false))).unwrap().num_rows(), 0);
        assert!(filter_owned(batch, &Expr::Column("missing".into())).is_err());
    }
}

#[cfg(test)]
mod literal_false_filter_fast_path_tests {
    use super::*;
    use crate::schema::{Schema, Field, DataType};
    #[test]
    fn false_and_unknown_filters_skip_large_column_values() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("🙂".repeat(100_000))],
            vec![Value::Null],
        ]).unwrap();
        for predicate in [Expr::Literal(Value::Boolean(false)), Expr::Literal(Value::Null)] {
            let empty = filter(&batch, &predicate).unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.schema(), &schema);
            assert_eq!(empty.column(0).unwrap(), &[]);
        }
    }
}

#[cfg(test)]
mod move_owned_filter_payloads {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn filtered_text_cells_keep_their_allocations() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let input = Batch::from_rows(schema, (0..100).map(|index| vec![
            if index % 7 == 0 { Value::Null } else { Value::Integer(index) },
            Value::Text(format!("{index}-{}", "é🙂".repeat(1000))),
        ]).collect()).unwrap();
        let ptr = match input.value(43, 1).unwrap() {
            Value::Text(value) => value.as_ptr(),
            _ => unreachable!(),
        };
        let predicate = Expr::binary(Expr::Column("id".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(40)));
        let expected = filter(&input, &predicate).unwrap();
        let moved = filter_owned(input, &predicate).unwrap();
        assert_eq!(moved, expected);
        assert_eq!(match moved.value(2, 1).unwrap() {
            Value::Text(value) => value.as_ptr(),
            _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn empty_and_identity_selections_keep_schema_and_rows() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Integer(1)], vec![Value::Integer(2)],
        ]).unwrap();
        let none = filter_owned(input.clone(),
            &Expr::Literal(Value::Boolean(false))).unwrap();
        assert_eq!(none.num_rows(), 0);
        assert_eq!(none.schema(), input.schema());
        let keep = Expr::binary(Expr::Column("id".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(0)));
        assert_eq!(filter_owned(input.clone(), &keep).unwrap(), input);
        assert!(filter_owned(input, &Expr::Column("id".into())).is_err());
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 4).unwrap();
        assert_eq!(filter_owned(zero, &Expr::Literal(Value::Boolean(true))).unwrap().num_rows(), 4);
    }
}

#[cfg(test)]
mod owned_literal_false_filter_fast_path_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn constant_false_and_unknown_drop_all_columns_without_changing_schema() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("🙂".repeat(4096))], vec![Value::Null],
        ]).unwrap();
        for predicate in [Value::Boolean(false), Value::Null] {
            let result = filter_owned(input.clone(), &Expr::Literal(predicate)).unwrap();
            assert_eq!(result.schema(), &schema);
            assert_eq!(result.num_rows(), 0);
            assert!(result.column(0).unwrap().is_empty());
        }
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 3).unwrap();
        assert_eq!(filter_owned(zero, &Expr::Literal(Value::Null)).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod bounded_boolean_column_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn direct_and_negated_boolean_columns_preserve_nulls_and_stop_at_limit() {
        let schema = Schema::new(vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let batch = Batch::from_rows(schema, vec![
            vec![Value::Null, Value::Text("a".into())],
            vec![Value::Boolean(false), Value::Text("b".into())],
            vec![Value::Boolean(true), Value::Text("c".repeat(512))],
            vec![Value::Boolean(false), Value::Null],
            vec![Value::Boolean(true), Value::Text("e".into())],
        ]).unwrap();
        let flag = Expr::Column("flag".into());
        assert_eq!(filter_first(&batch, &flag, 1).unwrap(),
            batch.take(&[2]).unwrap());
        assert_eq!(filter_first(&batch, &flag, 9).unwrap(),
            batch.take(&[2, 4]).unwrap());
        assert_eq!(filter_first(&batch, &Expr::Not(Box::new(flag)), 2).unwrap(),
            batch.take(&[1, 3]).unwrap());
        assert_eq!(filter_first(&batch, &Expr::Column("flag".into()), 0)
            .unwrap().num_rows(), 0);
        assert!(filter_first(&batch, &Expr::Column("text".into()), 0).is_err());
    }
}

#[cfg(test)]
mod dense_filter_selection_tests {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn dense_and_empty_selections_preserve_schema_and_rows() {
        let schema = Schema::new(vec![
            Field::new("ok", DataType::Boolean, true),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let predicate = Expr::Column("ok".into());
        let payload = "large-value".repeat(2000);
        let dense = Batch::from_rows(schema.clone(), vec![
            vec![Value::Boolean(true), Value::Text(payload.clone())],
            vec![Value::Boolean(true), Value::Text(payload)],
        ]).unwrap();
        assert_eq!(filter(&dense, &predicate).unwrap(), dense);
        assert_eq!(filter_owned(dense.clone(), &predicate).unwrap(), dense);
        let empty = Batch::from_rows(schema.clone(), vec![
            vec![Value::Boolean(false), Value::Text("a".into())],
            vec![Value::Null, Value::Text("b".into())],
        ]).unwrap();
        let borrowed = filter(&empty, &predicate).unwrap();
        let owned = filter_owned(empty, &predicate).unwrap();
        assert_eq!(borrowed, Batch::empty(schema.clone()));
        assert_eq!(owned, Batch::empty(schema));
    }
}

#[cfg(test)]
mod owned_short_circuit_filter_regressions {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn stops_before_unneeded_expression_errors_and_preserves_text_storage() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, false),
            Field::new("payload", DataType::Text, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Integer(1), Value::Text("🍃".repeat(3000))],
            vec![Value::Integer(0), Value::Text("bad".into())],
        ]).unwrap();
        let address = match input.value(0, 1).unwrap() {
            Value::Text(text) => text.as_ptr(), _ => unreachable!(),
        };
        let predicate = Expr::binary(
            Expr::binary(Expr::Literal(Value::Integer(10)), BinaryOp::Divide,
                         Expr::Column("n".into())),
            BinaryOp::Greater, Expr::Literal(Value::Integer(0)),
        );
        let result = filter_first_owned(input.clone(), &predicate, 1).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert!(filter_first_owned(input.clone(), &predicate, 2).is_err());
        let moved = filter_first_owned(input, &predicate, 1).unwrap();
        assert_eq!(match moved.value(0, 1).unwrap() {
            Value::Text(text) => text.as_ptr(), _ => unreachable!(),
        }, address);
    }

    #[test]
    fn nullable_boolean_columns_stop_at_match_budget() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("ok", DataType::Boolean, true),
        ]).unwrap(), vec![
            vec![Value::Null], vec![Value::Boolean(true)],
            vec![Value::Boolean(false)], vec![Value::Boolean(true)],
        ]).unwrap();
        let predicate = Expr::Column("ok".into());
        assert_eq!(filter_first_owned(input.clone(), &predicate, 1).unwrap().num_rows(), 1);
        assert_eq!(filter_first_owned(input.clone(), &Expr::Not(Box::new(predicate)), 2)
            .unwrap().rows().unwrap(), vec![vec![Value::Boolean(false)]]);
        assert_eq!(filter_first_owned(input, &Expr::Literal(Value::Boolean(false)), 1)
            .unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod direct_filter_expression_admission_regressions {
    use super::*;
    use crate::schema::{Field, Schema};

    #[test]
    fn library_filter_helpers_reject_deep_expressions_before_recursive_evaluation() {
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, false)]).unwrap();
        let input = Batch::from_rows(schema, vec![vec![Value::Boolean(true)]]).unwrap();
        let mut predicate = Expr::Literal(Value::Boolean(true));
        for _ in 0..80 { predicate = Expr::Not(Box::new(predicate)); }
        assert!(filter(&input, &predicate).unwrap_err().to_string().contains("complexity budget"));
        assert!(filter_owned(input.clone(), &predicate).unwrap_err().to_string().contains("complexity budget"));
        assert!(filter_first(&input, &predicate, 0).unwrap_err().to_string().contains("complexity budget"));
        assert!(filter_first_owned(input, &predicate, 0).unwrap_err().to_string().contains("complexity budget"));
    }
}
