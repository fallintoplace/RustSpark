use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};

pub fn split(batch: &Batch, rows_per_batch: usize) -> Result<Vec<Batch>> {
    if rows_per_batch == 0 {
        return Err(Error::Execution("batch size must be positive".into()));
    }
    crate::batch_chunks::chunks(batch, rows_per_batch)?.collect()
}

pub fn concat(
    schema: &Schema,
    batches: impl IntoIterator<Item = Batch>,
    max_rows: usize,
) -> Result<Batch> {
    let mut inputs = batches.into_iter();
    // Exact size hints are not guaranteed for filtered and streaming sources.
    // Inspect at most two owned batches before deciding whether concatenation
    // actually needs to duplicate any column buffers.
    let Some(first) = inputs.next() else { return Ok(Batch::empty(schema.clone())); };
    if first.schema() != schema {
        return Err(Error::Schema("cannot concatenate different batch schemas".into()));
    }
    if first.num_rows() > max_rows {
        return Err(Error::Execution("batch collection row limit exceeded".into()));
    }
    let Some(second) = inputs.next() else { return Ok(first); };
    // A provably exhausted iterator with one empty batch can move its
    // nonempty batch directly instead of duplicating all column values.
    if inputs.size_hint().1 == Some(0) {
        if second.schema() != schema {
            return Err(Error::Schema("cannot concatenate different batch schemas".into()));
        }
        if second.num_rows() == 0 {
            if first.num_rows() > max_rows {
                return Err(Error::Execution("batch collection row limit exceeded".into()));
            }
            return Ok(first);
        }
        if first.num_rows() == 0 {
            if second.num_rows() > max_rows {
                return Err(Error::Execution("batch collection row limit exceeded".into()));
            }
            return Ok(second);
        }
    }
    // The first owned batch already owns validated column buffers. Use
    // them as accumulation buffers instead of allocating one Vec per column
    // and copying the first batch into those fresh vectors.
    let mut total = first.num_rows();
    let mut columns = first.into_columns();
    for batch in std::iter::once(second).chain(inputs) {
        if batch.schema() != schema {
            return Err(Error::Schema("cannot concatenate different batch schemas".into()));
        }
        total = total.checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("batch row count overflow".into()))?;
        if total > max_rows {
            return Err(Error::Execution("batch collection row limit exceeded".into()));
        }
        // The iterator owns each batch. Move its column values rather than
        // deep-cloning TEXT payloads for every concatenated row.
        for (output, values) in columns.iter_mut().zip(batch.into_columns()) {
            output.extend(values);
        }
    }
    // All inputs were validated Batch instances with exactly this schema.
    // Preserve the shape check without scanning their scalar values again.
    Batch::from_validated_columns(schema.clone(), columns, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field}};

    #[test]
    fn split_concat_preserves_order_and_rejects_overflow() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema.clone(), (0..7)
            .map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let chunks = split(&batch, 3).unwrap();
        assert_eq!(chunks.iter().map(Batch::num_rows).collect::<Vec<_>>(), [3, 3, 1]);
        assert!(concat(&schema, chunks.clone(), 6).is_err());
        assert_eq!(concat(&schema, chunks, 7).unwrap(), batch);
        assert!(split(&batch, 0).is_err());
    }

    #[test]
    fn concat_handles_empty_nullable_columns_and_repeated_chunks() {
        let schema = Schema::new(vec![
            Field::new("n", DataType::Integer, true),
            Field::new("s", DataType::Text, true),
        ]).unwrap();
        let rows = vec![
            vec![Value::Null, Value::Text("alpha".into())],
            vec![Value::Integer(7), Value::Null],
        ];
        let input = Batch::from_rows(schema.clone(), rows.clone()).unwrap();
        let result = concat(&schema, [Batch::empty(schema.clone()), input], 2).unwrap();
        assert_eq!(result.rows().unwrap(), rows);
    }

    #[test]
    fn concatenate_rejects_schema_changes() {
        let a = Schema::new(vec![Field::new("a", DataType::Text, true)]).unwrap();
        let b = Schema::new(vec![Field::new("b", DataType::Text, true)]).unwrap();
        assert!(concat(&a, [Batch::empty(b)], 10).is_err());
    }
}

#[cfg(test)]
mod zero_column_concat_consolidation_tests {
    use super::*;
    #[test]
    fn concat_preserves_zero_column_row_counts() {
        let schema = Schema::new(vec![]).unwrap();
        let a = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let b = Batch::new_with_row_count(schema.clone(), vec![], 3).unwrap();
        assert_eq!(concat(&schema, [a.clone(), b.clone()], 5).unwrap().num_rows(), 5);
        assert!(concat(&schema, [a, b], 4).is_err());
    }
}

#[cfg(test)]
mod split_contiguous_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn single_row_and_empty_width_chunks_preserve_cardinality() {
        let schema = Schema::new(vec![]).unwrap();
        let input = Batch::new_with_row_count(schema, vec![], 5).unwrap();
        let parts = split(&input, 2).unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 2, 1]);
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let data = Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(1)]]).unwrap();
        assert_eq!(split(&data, 1).unwrap()[1].rows().unwrap(), vec![vec![Value::Integer(1)]]);
    }
}

#[cfg(test)]
mod preallocated_batch_split_tests {
    use super::*;
    #[test]
    fn preserves_empty_and_nondivisible_batch_splits() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "n", crate::schema::DataType::Integer, false)]).unwrap();
        let empty = Batch::empty(schema.clone());
        assert!(split(&empty, 10).unwrap().is_empty());
        let populated = Batch::from_rows(schema,
            (0..13).map(|n| vec![Value::Integer(n)]).collect()).unwrap();
        let chunks = split(&populated, 5).unwrap();
        assert_eq!(chunks.iter().map(Batch::num_rows).collect::<Vec<_>>(), [5, 5, 3]);
    }
}

#[cfg(test)]
mod single_owned_batch_concat_tests {
    use super::*;
    #[test]
    fn one_batch_keeps_values_schema_and_budget_checks() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "text", crate::schema::DataType::Text, true)]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("café".into())], vec![Value::Null]
        ]).unwrap();
        assert_eq!(concat(&schema, [first.clone()], 2).unwrap(), first);
        assert!(concat(&schema, [first.clone()], 1).is_err());
        let other_schema = Schema::new(vec![crate::schema::Field::new(
            "different", crate::schema::DataType::Text, true)]).unwrap();
        assert!(concat(&other_schema, [first], 5).is_err());
    }
}

#[cfg(test)]
mod inexact_single_batch_concat_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn inexact_iterators_can_pass_owned_single_batches_through() {
        let schema = Schema::new(vec![
            Field::new("word", DataType::Text, true),
        ]).unwrap();
        let original = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("é🙂".repeat(20_000))], vec![Value::Null],
        ]).unwrap();
        let iterator = std::iter::once(original.clone()).filter(|_| true);
        assert_eq!(concat(&schema, iterator, 2).unwrap(), original);
        assert!(concat(&schema, std::iter::once(original.clone()).filter(|_| true), 1).is_err());

        let zero_width = Schema::new(vec![]).unwrap();
        let one = Batch::new_with_row_count(zero_width.clone(), vec![], 5).unwrap();
        assert_eq!(concat(&zero_width, std::iter::once(one).filter(|_| true), 5)
            .unwrap().num_rows(), 5);
        let empty = concat(&zero_width, std::iter::empty(), 0).unwrap();
        assert_eq!(empty.num_rows(), 0);
    }
}

#[cfg(test)]
mod empty_pair_concat_passthrough_tests {
    use super::*;
    use crate::{schema::{DataType, Field}, scalar::Value};

    #[test]
    fn moves_nonempty_batch_past_empty_prefix_or_suffix() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let populated = Batch::from_rows(schema.clone(),
            vec![vec![Value::Text("🙂".repeat(20_000))], vec![Value::Null]]).unwrap();
        let empty = Batch::empty(schema.clone());
        assert_eq!(concat(&schema, [empty.clone(), populated.clone()], 2).unwrap(), populated);
        assert_eq!(concat(&schema, [populated.clone(), empty], 2).unwrap(), populated);
        assert!(concat(&schema, [Batch::empty(schema.clone()), populated.clone()], 1).is_err());
        assert!(concat(&schema, [populated, Batch::empty(schema.clone())], 1).is_err());
        let mismatched = Schema::new(vec![Field::new("other", DataType::Text, true)]).unwrap();
        assert!(concat(&schema, [Batch::empty(schema.clone()), Batch::empty(mismatched)], 2).is_err());
    }
}

#[cfg(test)]
mod owned_concat_move_regressions {
    use super::*;
    use crate::schema::{DataType, Field};

    #[test]
    fn concatenation_reuses_allocations_of_owned_text_values() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
            Field::new("key", DataType::Integer, false),
        ]).unwrap();
        let left = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("é🙂".repeat(4096)), Value::Integer(1)],
        ]).unwrap();
        let right = Batch::from_rows(schema.clone(), vec![
            vec![Value::Null, Value::Integer(2)],
            vec![Value::Text("payload".repeat(2048)), Value::Integer(3)],
        ]).unwrap();
        let left_ptr = match &left.column(0).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!()
        };
        let right_ptr = match &right.column(0).unwrap()[1] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!()
        };
        let result = concat(&schema, [left, right], 3).unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.column(1).unwrap(), &[
            Value::Integer(1), Value::Integer(2), Value::Integer(3)
        ]);
        assert_eq!(match &result.column(0).unwrap()[0] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!()
        }, left_ptr);
        assert_eq!(match &result.column(0).unwrap()[2] {
            Value::Text(value) => value.as_ptr(), _ => unreachable!()
        }, right_ptr);
    }

    #[test]
    fn owned_concat_still_checks_limits_and_zero_width_rows() {
        let schema = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(schema.clone(), vec![], 3).unwrap();
        let right = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        assert_eq!(concat(&schema, [left.clone(), right.clone()], 5).unwrap().num_rows(), 5);
        assert!(concat(&schema, [left, right], 4).is_err());
    }
}

#[cfg(test)]
mod validated_owned_concatenation_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn many_nullable_text_chunks_move_values_and_keep_row_budget() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let inputs = (0..73).map(|n| Batch::from_rows(schema.clone(), vec![vec![
            if n % 13 == 0 { Value::Null } else { Value::Text(format!("{n}-{}", "x".repeat(1024))) },
            Value::Integer(n),
        ]]).unwrap()).collect::<Vec<_>>();
        let expected = inputs.iter().flat_map(|b| b.rows().unwrap()).collect::<Vec<_>>();
        let combined = concat(&schema, inputs.clone(), 73).unwrap();
        assert_eq!(combined.rows().unwrap(), expected);
        assert!(concat(&schema, inputs, 72).is_err());
        let empty_schema = Schema::new(vec![]).unwrap();
        let parts = (0..19).map(|_| Batch::new_with_row_count(
            empty_schema.clone(), vec![], 7).unwrap()).collect::<Vec<_>>();
        assert_eq!(concat(&empty_schema, parts, 133).unwrap().num_rows(), 133);
    }
}

#[cfg(test)]
mod reuse_first_concat_column_buffers_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn multiple_owned_inputs_preserve_payloads_and_cardinality() {
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Text, true),
        ]).unwrap();
        let first = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("💡".repeat(4096))], vec![Value::Null],
        ]).unwrap();
        let second = Batch::from_rows(schema.clone(), vec![
            vec![Value::Text("tail".into())],
        ]).unwrap();
        let first_ptr = match first.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        };
        let output = concat(&schema, [first, second], 3).unwrap();
        assert_eq!(output.num_rows(), 3);
        assert_eq!(match output.value(0, 0).unwrap() {
            Value::Text(text) => text.as_ptr(),
            _ => unreachable!(),
        }, first_ptr);
        assert_eq!(output.value(1, 0).unwrap(), &Value::Null);
        assert_eq!(output.value(2, 0).unwrap(), &Value::Text("tail".into()));

        let zero_width = Schema::new(vec![]).unwrap();
        let batches = (1..=4).map(|count|
            Batch::new_with_row_count(zero_width.clone(), vec![], count).unwrap());
        assert_eq!(concat(&zero_width, batches, 10).unwrap().num_rows(), 10);
    }
}
