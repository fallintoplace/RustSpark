use crate::{
    batch::Batch,
    expr::Expr,
    scalar::Value,
    schema::{Field, Schema},
    Error, Result,
};

const MAX_MATERIALIZED_PROJECT_BYTES: usize = 64 * 1024 * 1024;

fn charge_projected_value(used: &mut usize, value: &Value, copies: usize) -> Result<()> {
    let payload = if let Value::Text(text) = value { text.len() } else { 0 };
    let per_value = std::mem::size_of::<Value>().checked_add(payload)
        .ok_or_else(|| Error::Execution("projection size overflow".into()))?;
    let total = per_value.checked_mul(copies)
        .ok_or_else(|| Error::Execution("projection size overflow".into()))?;
    *used = used.checked_add(total)
        .filter(|&bytes| bytes <= MAX_MATERIALIZED_PROJECT_BYTES)
        .ok_or_else(|| Error::Execution("projection materialization budget exceeded".into()))?;
    Ok(())
}


#[derive(Clone, Debug, PartialEq)]
pub struct Selection {
    pub expression: Expr,
    pub alias: String,
}

impl Selection {
    pub fn new(expression: Expr, alias: impl Into<String>) -> Self {
        Self {
            expression,
            alias: alias.into(),
        }
    }
}

fn expression_nullable(expression: &Expr, schema: &Schema) -> Result<bool> {
    match expression {
        Expr::Column(name) => Ok(schema.field(schema.column(name)?)?.nullable),
        Expr::Literal(value) => Ok(value.is_null()),
        Expr::IsNull(_) | Expr::IsDistinctFrom { .. } => Ok(false),
        Expr::Not(inner) if matches!(inner.as_ref(),
            Expr::IsNull(_) | Expr::IsDistinctFrom { .. }) => Ok(false),
        _ => Ok(true),
    }
}

/// Bind predicates on a source column once, rather than cloning each
/// TEXT scalar and resolving its name again for every projected row.
fn bound_null_test<'a>(
    batch: &'a Batch, expression: &Expr,
) -> Result<Option<(&'a [Value], bool)>> {
    let (inner, is_null) = match expression {
        Expr::IsNull(inner) => (inner.as_ref(), true),
        Expr::Not(inner) => match inner.as_ref() {
            Expr::IsNull(column) => (column.as_ref(), false),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    let Expr::Column(name) = inner else { return Ok(None); };
    let index = batch.schema().column(name)?;
    Ok(Some((batch.column(index)?, is_null)))
}

fn bound_distinct_comparison<'a>(
    batch: &'a Batch, expression: &'a Expr,
) -> Result<Option<(&'a [Value], &'a Value, bool)>> {
    let (expression, inverse) = match expression {
        Expr::Not(inner) => (inner.as_ref(), true),
        _ => (expression, false),
    };
    let Expr::IsDistinctFrom { left, right, negated } = expression else {
        return Ok(None);
    };
    let (name, literal) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(name), Expr::Literal(value)) |
        (Expr::Literal(value), Expr::Column(name)) => (name, value),
        _ => return Ok(None),
    };
    let index = batch.schema().column(name)?;
    Ok(Some((batch.column(index)?, literal, *negated ^ inverse)))
}

pub fn project(batch: &Batch, selections: &[Selection]) -> Result<Batch> {
    selections.iter().try_for_each(|item| item.expression.validate_budget())?;
    let schema = Schema::new(
        selections
            .iter()
            .map(|selection| {
                Ok(Field::new(
                    &selection.alias,
                    selection.expression.output_type(batch.schema())?,
                    expression_nullable(&selection.expression, batch.schema())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?,
    )?;
    let mut columns: Vec<Vec<Value>> = selections
        .iter()
        .map(|_| Vec::with_capacity(batch.num_rows()))
        .collect();
    let mut generated_bytes = 0usize;
    for (column, selection) in columns.iter_mut().zip(selections) {
        if let Expr::Column(name) = &selection.expression {
            let index = batch.schema().column(name)?;
            for value in batch.column(index)? {
                charge_projected_value(&mut generated_bytes, value, 1)?;
            }
            column.extend_from_slice(batch.column(index)?);
        } else if let Expr::Literal(value) = &selection.expression {
            charge_projected_value(&mut generated_bytes, value, batch.num_rows())?;
            column.resize(batch.num_rows(), value.clone());
        } else if let Some((values, is_null)) =
            bound_null_test(batch, &selection.expression)? {
            charge_projected_value(&mut generated_bytes, &Value::Boolean(false), batch.num_rows())?;
            column.extend(values.iter().map(|value|
                Value::Boolean(value.is_null() == is_null)));
        } else if let Some((values, literal, negated)) =
            bound_distinct_comparison(batch, &selection.expression)? {
            charge_projected_value(&mut generated_bytes, &Value::Boolean(false), values.len())?;
            for value in values {
                let distinct = match (value.is_null(), literal.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    (false, false) => value.sql_equal(literal)?
                        .map(|equal| !equal)
                        .unwrap_or_else(|| value != literal),
                };
                column.push(Value::Boolean(if negated { !distinct } else { distinct }));
            }
        } else {
            for row in 0..batch.num_rows() {
                let value = selection.expression.evaluate(batch, row)?;
                charge_projected_value(&mut generated_bytes, &value, 1)?;
                column.push(value);
            }
        }
    }
    Batch::new_with_row_count(schema, columns, batch.num_rows())
}


/** Project an owned batch without cloning direct source columns.
 *
 * Computed expressions are evaluated before moving the input columns.
 * Repeated references clone only the earlier occurrences; the last use
 * consumes the original column and preserves heap-backed TEXT allocations.
 */
pub fn project_owned(batch: Batch, selections: &[Selection]) -> Result<Batch> {
    selections.iter().try_for_each(|item| item.expression.validate_budget())?;
    let schema = Schema::new(selections.iter().map(|selection| {
        Ok(Field::new(
            &selection.alias,
            selection.expression.output_type(batch.schema())?,
            expression_nullable(&selection.expression, batch.schema())?,
        ))
    }).collect::<Result<Vec<_>>>()?)?;
    let rows = batch.num_rows();
    let mut generated_bytes = 0usize;
    let mut sources = Vec::with_capacity(selections.len());
    let mut prepared = Vec::with_capacity(selections.len());
    let mut remaining = vec![0usize; batch.schema().len()];
    for selection in selections {
        match &selection.expression {
            Expr::Column(name) => {
                let index = batch.schema().column(name)?;
                remaining[index] += 1;
                sources.push(Some(index));
                prepared.push(None);
            }
            Expr::Literal(value) => {
                sources.push(None);
                charge_projected_value(&mut generated_bytes, value, rows)?;
                prepared.push(Some(vec![value.clone(); rows]));
            }
            expression => {
                sources.push(None);
                let values = if let Some((column, is_null)) =
                    bound_null_test(&batch, expression)? {
                    charge_projected_value(&mut generated_bytes, &Value::Boolean(false), rows)?;
                    column.iter().map(|value|
                        Value::Boolean(value.is_null() == is_null)).collect()
                } else if let Some((column, literal, negated)) =
                    bound_distinct_comparison(&batch, expression)? {
                    charge_projected_value(&mut generated_bytes, &Value::Boolean(false), rows)?;
                    column.iter().map(|value| {
                        let distinct = match (value.is_null(), literal.is_null()) {
                            (true, true) => false,
                            (true, false) | (false, true) => true,
                            (false, false) => value.sql_equal(literal)?
                                .map(|equal| !equal)
                                .unwrap_or_else(|| value != literal),
                        };
                        Ok(Value::Boolean(if negated { !distinct } else { distinct }))
                    }).collect::<Result<Vec<_>>>()?
                } else {
                    let mut values = Vec::with_capacity(rows);
                    for row in 0..rows {
                        let value = expression.evaluate(&batch, row)?;
                        charge_projected_value(&mut generated_bytes, &value, 1)?;
                        values.push(value);
                    }
                    values
                };
                prepared.push(Some(values));
            }
        }
    }
    let mut owned = batch.into_columns().into_iter().map(Some).collect::<Vec<_>>();
    let columns = prepared.into_iter().zip(sources).map(|(values, source)| {
        if let Some(index) = source {
            remaining[index] -= 1;
            if remaining[index] == 0 {
                owned[index].take().expect("last source projection owns its column")
            } else {
                owned[index].as_ref().expect("source projection is available").clone()
            }
        } else {
            values.expect("computed projection already evaluated")
        }
    }).collect();
    Batch::new_with_row_count(schema, columns, rows)
}

#[cfg(test)]
mod owned_projection_regressions {
    use super::*;
    use crate::{expr::BinaryOp, schema::{DataType, Field, Schema}};

    #[test]
    fn moves_large_text_columns_and_preserves_computed_results() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("é🙂".repeat(2048)), Value::Integer(2)],
            vec![Value::Null, Value::Integer(4)],
        ]).unwrap();
        let ptr = match input.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let selections = [
            Selection::new(Expr::Column("payload".into()), "body"),
            Selection::new(Expr::binary(
                Expr::Column("id".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(1)),
            ), "plus_one"),
        ];
        let expected = project(&input, &selections).unwrap();
        let output = project_owned(input, &selections).unwrap();
        assert_eq!(output, expected);
        assert_eq!(match output.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn repeated_aliases_keep_values_and_move_last_reference() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("hello".repeat(500))],
            vec![Value::Null],
        ]).unwrap();
        let ptr = match input.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let selections = [
            Selection::new(Expr::Column("text".into()), "first"),
            Selection::new(Expr::Column("TEXT".into()), "last"),
        ];
        let expected = project(&input, &selections).unwrap();
        let output = project_owned(input, &selections).unwrap();
        assert_eq!(output, expected);
        assert_eq!(match output.value(0, 1).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, ptr);
        assert!(project_owned(output, &[Selection::new(
            Expr::Column("missing".into()), "bad",
        )]).is_err());
    }

    #[test]
    fn zero_width_projection_preserves_row_count() {
        let input = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        assert_eq!(project_owned(input, &[]).unwrap().num_rows(), 5);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::BinaryOp,
        scalar::Value,
        schema::{DataType, Field},
    };

    #[test]
    fn computed_columns_are_typed_and_aliased() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(4)]],
        )
        .unwrap();
        let result = project(
            &batch,
            &[Selection::new(
                Expr::binary(
                    Expr::Column("x".into()),
                    BinaryOp::Add,
                    Expr::Literal(Value::Integer(2)),
                ),
                "plus_two",
            )],
        )
        .unwrap();
        assert_eq!(result.schema().column("plus_two").unwrap(), 0);
        assert_eq!(result.row(0).unwrap(), vec![Value::Integer(6)]);
    }
}

 
/// Select existing column vectors without evaluating scalar expressions.
/// Unlike computed projection this preserves source field nullability.
pub fn project_columns(batch: &Batch, names: &[&str]) -> Result<Batch> {
    let mut fields = Vec::with_capacity(names.len());
    let mut values = Vec::with_capacity(names.len());
    for name in names {
        let position = batch.schema().column(name)?;
        fields.push(batch.schema().field(position)?.clone());
        values.push(batch.column(position)?.to_vec());
    }
    Batch::new_with_row_count(Schema::new(fields)?, values, batch.num_rows())
}

#[cfg(test)]
mod typed_projection_feature_tests {
    use super::*;
    use crate::{schema::{DataType, Field}, scalar::Value};

    #[test]
    fn keeps_schema_constraints_and_requested_order() {
        let input = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("note", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(3), Value::Null],
        ]).unwrap();
        let result = project_columns(&input, &["note", "id"]).unwrap();
        assert_eq!(result.row(0).unwrap(), vec![Value::Null, Value::Integer(3)]);
        assert!(result.schema().field(1).unwrap().nullable == false);
        assert!(project_columns(&input, &["id", "ID"]).is_err());
        assert!(project_columns(&input, &["unknown"]).is_err());
    }
}


#[cfg(test)]
mod selected_nullability_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn projects_required_column_and_nonnull_literal_as_not_nullable() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(42)]],
        ).unwrap();
        let result = project(&input, &[
            Selection::new(Expr::Column("id".into()), "key"),
            Selection::new(Expr::Literal(Value::Integer(1)), "one"),
            Selection::new(Expr::Literal(Value::Null), "missing"),
        ]).unwrap();
        assert!(!result.schema().field(0).unwrap().nullable);
        assert!(!result.schema().field(1).unwrap().nullable);
        assert!(result.schema().field(2).unwrap().nullable);
    }
}

#[cfg(test)]
mod zero_column_projection_consolidation_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn both_projection_paths_keep_zero_width_row_counts() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let source = Batch::from_rows(schema, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]).unwrap();
        for projected in [project(&source, &[]).unwrap(), project_columns(&source, &[]).unwrap()] {
            assert_eq!(projected.schema().len(), 0);
            assert_eq!(projected.rows().unwrap(), vec![vec![]; 2]);
        }
    }
}

#[cfg(test)]
mod direct_column_projection_perf_tests {
    use super::*;
    use crate::{expr::BinaryOp, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn column_fast_path_preserves_aliases_duplicates_and_computed_values() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Integer(3)]],
        ).unwrap();
        let result = project(&input, &[
            Selection::new(Expr::Column("n".into()), "first"),
            Selection::new(Expr::Column("n".into()), "again"),
            Selection::new(Expr::binary(
                Expr::Column("n".into()), BinaryOp::Add,
                Expr::Literal(Value::Integer(2)),
            ), "computed"),
        ]).unwrap();
        assert_eq!(result.rows().unwrap(), vec![
            vec![Value::Null, Value::Null, Value::Null],
            vec![Value::Integer(3), Value::Integer(3), Value::Integer(5)],
        ]);
        assert!(project(&input, &[Selection::new(
            Expr::Column("unknown".into()), "missing"
        )]).is_err());
    }
}

#[cfg(test)]
mod projection_fast_paths_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn direct_columns_and_literals_match_scalar_evaluation() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("name", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Text("a".into())], vec![Value::Null]],
        ).unwrap();
        let selections = [
            Selection::new(Expr::Column("name".into()), "first"),
            Selection::new(Expr::Column("NAME".into()), "again"),
            Selection::new(Expr::Literal(Value::Integer(7)), "constant"),
            Selection::new(Expr::Literal(Value::Null), "missing"),
        ];
        let projected = project(&input, &selections).unwrap();
        assert_eq!(projected.rows().unwrap(), vec![
            vec![Value::Text("a".into()), Value::Text("a".into()), Value::Integer(7), Value::Null],
            vec![Value::Null, Value::Null, Value::Integer(7), Value::Null],
        ]);
        assert_eq!(project(&Batch::empty(input.schema().clone()), &selections).unwrap().num_rows(), 0);
    }
}

#[cfg(test)]
mod borrowed_projection_predicate_regressions {
    use super::*;

    #[test]
    fn large_nullable_text_predicates_match_scalar_evaluation() {
        let schema = Schema::new(vec![
            Field::new("message", crate::schema::DataType::Text, true),
        ]).unwrap();
        let wide = "🍕".repeat(4096);
        let rows = (0..201).map(|n| vec![
            if n % 11 == 0 { Value::Null }
            else if n % 7 == 0 { Value::Text("other".into()) }
            else { Value::Text(wide.clone()) },
        ]).collect();
        let source = Batch::from_rows(schema, rows).unwrap();
        let col = || Expr::Column("message".into());
        let selections = vec![
            Selection::new(Expr::IsNull(Box::new(col())), "isnull"),
            Selection::new(Expr::Not(Box::new(Expr::IsNull(Box::new(col())))), "notnull"),
            Selection::new(Expr::IsDistinctFrom {
                left: Box::new(col()), right: Box::new(Expr::Literal(Value::Text(wide.clone()))),
                negated: false,
            }, "distinct"),
            Selection::new(Expr::IsDistinctFrom {
                left: Box::new(Expr::Literal(Value::Text(wide))),
                right: Box::new(col()), negated: true,
            }, "same"),
            Selection::new(Expr::IsDistinctFrom {
                left: Box::new(col()), right: Box::new(Expr::Literal(Value::Null)),
                negated: false,
            }, "notnull_distinct"),
        ];
        let actual = project(&source, &selections).unwrap();
        for (position, selection) in selections.iter().enumerate() {
            assert_eq!(actual.schema().field(position).unwrap().nullable, false);
            for row in 0..source.num_rows() {
                assert_eq!(actual.value(row, position).unwrap(),
                    &selection.expression.evaluate(&source, row).unwrap());
            }
        }
    }

    #[test]
    fn floating_distinct_comparison_keeps_nan_and_signed_zero_semantics() {
        let schema = Schema::new(vec![
            Field::new("key", crate::schema::DataType::Float, true),
        ]).unwrap();
        let data = Batch::from_rows(schema, vec![
            vec![Value::Float(f64::NAN)],
            vec![Value::Float(-0.0)],
            vec![Value::Float(0.0)],
            vec![Value::Null],
        ]).unwrap();
        let expression = Expr::IsDistinctFrom {
            left: Box::new(Expr::Column("key".into())),
            right: Box::new(Expr::Literal(Value::Float(0.0))),
            negated: false,
        };
        let projected = project(&data, &[Selection::new(expression, "distinct")]).unwrap();
        assert_eq!(projected.column(0).unwrap(), &[
            Value::Boolean(true), Value::Boolean(false),
            Value::Boolean(false), Value::Boolean(true),
        ]);
    }
}

#[cfg(test)]
mod owned_bound_predicate_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn nullable_long_text_predicates_match_borrowed_projection_and_move_payload() {
        let large = "é🙂".repeat(4096);
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("key", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Text(large.clone())],
            vec![Value::Null],
            vec![Value::Text("different".into())],
        ]).unwrap();
        let ptr = match batch.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let key = || Expr::Column("key".into());
        let null_test = || Expr::IsNull(Box::new(key()));
        let distinct = |negated| Expr::IsDistinctFrom {
            left: Box::new(key()),
            right: Box::new(Expr::Literal(Value::Text(large.clone()))),
            negated,
        };
        let selections = [
            Selection::new(null_test(), "is_null"),
            Selection::new(Expr::Not(Box::new(null_test())), "is_not_null"),
            Selection::new(distinct(false), "different"),
            Selection::new(distinct(true), "same"),
            Selection::new(key(), "retained"),
        ];
        let expected = project(&batch, &selections).unwrap();
        let actual = project_owned(batch, &selections).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.rows().unwrap()[1][0], Value::Boolean(true));
        assert_eq!(actual.rows().unwrap()[1][2], Value::Boolean(true));
        assert_eq!(actual.rows().unwrap()[0][3], Value::Boolean(true));
        assert_eq!(match actual.value(0, 4).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, ptr);
    }

    #[test]
    fn null_safe_float_and_null_comparisons_remain_exact() {
        let batch = Batch::from_rows(Schema::new(vec![
            Field::new("x", DataType::Float, true),
        ]).unwrap(), vec![
            vec![Value::Float(f64::NAN)],
            vec![Value::Float(-0.0)],
            vec![Value::Null],
        ]).unwrap();
        let selections = [
            Selection::new(Expr::IsDistinctFrom {
                left: Box::new(Expr::Column("x".into())),
                right: Box::new(Expr::Literal(Value::Float(f64::NAN))),
                negated: false,
            }, "different"),
            Selection::new(Expr::IsDistinctFrom {
                left: Box::new(Expr::Column("x".into())),
                right: Box::new(Expr::Literal(Value::Null)),
                negated: true,
            }, "null_eq"),
        ];
        assert_eq!(project_owned(batch.clone(), &selections).unwrap(),
            project(&batch, &selections).unwrap());
    }
}


/** Select a bounded direct-scan window without materializing unused columns.
 *
 * Computed projections return None because evaluating discarded rows may fail.
 * A caller must retain the normal projection operator for those expressions.
 */
pub(crate) fn project_columns_window(
    batch: &Batch, selections: &[Selection], offset: usize, count: Option<usize>,
) -> Result<Option<Batch>> {
    if !selections.iter().all(|selection|
        matches!(&selection.expression, Expr::Column(_))) {
        return Ok(None);
    }
    if count.is_some_and(|count| offset.checked_add(count).is_none()) {
        return Err(crate::Error::Execution("LIMIT + OFFSET overflow".into()));
    }
    let start = offset.min(batch.num_rows());
    let end = count.map_or(batch.num_rows(), |count|
        start.saturating_add(count).min(batch.num_rows()));
    let mut indices = Vec::with_capacity(selections.len());
    let mut fields = Vec::with_capacity(selections.len());
    for selection in selections {
        let Expr::Column(name) = &selection.expression else {
            unreachable!("checked column-only projection");
        };
        let index = batch.schema().column(name)?;
        let field = batch.schema().field(index)?;
        indices.push(index);
        fields.push(Field::new(&selection.alias, field.data_type, field.nullable));
    }
    // Validate aliases before cloning any selected TEXT values.
    let schema = Schema::new(fields)?;
    let columns = indices.into_iter().map(|index| {
        Ok(batch.column(index)?[start..end].to_vec())
    }).collect::<Result<Vec<_>>>()?;
    Ok(Some(Batch::from_validated_columns(schema, columns, end - start)?))
}

#[cfg(test)]
mod projected_catalog_window_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn direct_window_keeps_alias_order_nullability_and_empty_width_rows() {
        let source = Batch::from_rows(Schema::new(vec![
            Field::new("id", DataType::Integer, false),
            Field::new("text", DataType::Text, true),
        ]).unwrap(), vec![
            vec![Value::Integer(1), Value::Null],
            vec![Value::Integer(2), Value::Text("hello".repeat(1024))],
            vec![Value::Integer(3), Value::Null],
        ]).unwrap();
        let selections = [
            Selection::new(Expr::Column("text".into()), "label"),
            Selection::new(Expr::Column("ID".into()), "key"),
        ];
        let expected = crate::limit::limit(&project(&source, &selections).unwrap(),
            1, Some(2)).unwrap();
        let output = project_columns_window(&source, &selections, 1, Some(2))
            .unwrap().unwrap();
        assert_eq!(output, expected);
        assert_eq!(project_columns_window(&source, &[], 1, Some(2))
            .unwrap().unwrap().num_rows(), 2);
        assert!(project_columns_window(&source, &selections, usize::MAX, Some(1)).is_err());
        assert!(project_columns_window(&source, &[
            Selection::new(Expr::Column("missing".into()), "bad"),
        ], 20, Some(1)).is_err());
        assert!(project_columns_window(&source, &[
            Selection::new(Expr::Column("id".into()), "x"),
            Selection::new(Expr::Column("text".into()), "X"),
        ], 0, Some(1)).is_err());
    }
}

#[cfg(test)]
mod negated_nullsafe_projection_tests {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn negated_nullsafe_projection_matches_scalar_nullable_float_oracle() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float, true)]).unwrap();
        let source = Batch::from_rows(schema, vec![
            vec![Value::Null], vec![Value::Float(-0.0)],
            vec![Value::Float(0.0)], vec![Value::Float(f64::NAN)],
            vec![Value::Float(5.0)],
        ]).unwrap();
        for literal in [Value::Null, Value::Float(0.0), Value::Float(f64::NAN)] {
            for negated in [false, true] {
                let expression = Expr::Not(Box::new(Expr::IsDistinctFrom {
                    left: Box::new(Expr::Column("value".into())),
                    right: Box::new(Expr::Literal(literal.clone())),
                    negated,
                }));
                let selections = [Selection::new(expression.clone(), "matched")];
                let expected = (0..source.num_rows())
                    .map(|row| expression.evaluate(&source, row).unwrap())
                    .collect::<Vec<_>>();
                for actual in [
                    project(&source, &selections).unwrap(),
                    project_owned(source.clone(), &selections).unwrap(),
                ] {
                    assert_eq!(actual.column(0).unwrap(), expected.as_slice());
                    assert!(!actual.schema().field(0).unwrap().nullable);
                }
            }
        }
    }

    #[test]
    fn negated_unicode_text_projection_matches_scalar_oracle() {
        let long = "é🐈".repeat(2048);
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let source = Batch::from_rows(schema, vec![
            vec![Value::Text(long.clone())], vec![Value::Null],
            vec![Value::Text("no".into())], vec![Value::Text(long.clone())],
        ]).unwrap();
        let expression = Expr::Not(Box::new(Expr::IsDistinctFrom {
            left: Box::new(Expr::Column("text".into())),
            right: Box::new(Expr::Literal(Value::Text(long))),
            negated: false,
        }));
        let selections = [Selection::new(expression.clone(), "match")];
        let expected = (0..source.num_rows())
            .map(|row| expression.evaluate(&source, row).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(project(&source, &selections).unwrap().column(0).unwrap(), expected);
        assert_eq!(project_owned(source, &selections).unwrap().column(0).unwrap(), expected);
    }
}

#[cfg(test)]
mod projection_computed_memory_budget_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn literal_text_expansion_is_rejected_before_allocating_large_output() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = Batch::from_rows(schema, (0..65).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let selections = [Selection::new(Expr::Literal(Value::Text("p".repeat(1_048_576))), "payload")];
        let borrowed = project(&input, &selections).unwrap_err().to_string();
        assert!(borrowed.contains("projection materialization budget"));
        let owned = project_owned(input, &selections).unwrap_err().to_string();
        assert!(owned.contains("projection materialization budget"));
    }

    #[test]
    fn small_computed_and_borrowed_columns_match_existing_results() {
        let schema = Schema::new(vec![Field::new("name", DataType::Text, false)]).unwrap();
        let input = Batch::from_rows(schema, vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]).unwrap();
        let selections = [Selection::new(Expr::Column("name".into()), "original"),
            Selection::new(Expr::Literal(Value::Text("same".into())), "fixed")];
        assert_eq!(project(&input, &selections).unwrap(), project_owned(input, &selections).unwrap());
    }
}

#[cfg(test)]
mod direct_project_expression_budget_tests {
    use super::*;

    #[test]
    fn oversized_projection_is_rejected_before_inference_on_empty_input() {
        let empty = Batch::empty(Schema::new(vec![]).unwrap());
        let mut expression = Expr::Literal(Value::Boolean(true));
        for _ in 0..80 { expression = Expr::Not(Box::new(expression)); }
        let selection = Selection::new(expression, "deep");
        for result in [
            project(&empty, &[selection.clone()]),
            project_owned(empty.clone(), &[selection]),
        ] {
            assert!(result.unwrap_err().to_string().contains("complexity budget"));
        }
    }
}
