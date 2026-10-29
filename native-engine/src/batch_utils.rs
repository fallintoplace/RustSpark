use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};

pub fn split(batch: &Batch, rows_per_batch: usize) -> Result<Vec<Batch>> {
    if rows_per_batch == 0 {
        return Err(Error::Execution("batch size must be positive".into()));
    }
    if batch.num_rows() > 0 && batch.num_rows() <= rows_per_batch {
        return Ok(vec![batch.clone()]);
    }
    let mut chunks = Vec::with_capacity(batch.num_rows().div_ceil(rows_per_batch));
    let mut start = 0;
    while start < batch.num_rows() {
        let end = start.saturating_add(rows_per_batch).min(batch.num_rows());
        chunks.push(batch.slice_rows(start, end)?);
        start = end;
    }
    Ok(chunks)
}

pub fn concat(
    schema: &Schema,
    batches: impl IntoIterator<Item = Batch>,
    max_rows: usize,
) -> Result<Batch> {
    let mut inputs = batches.into_iter();
    if inputs.size_hint() == (1, Some(1)) {
        if let Some(batch) = inputs.next() {
            if batch.schema() != schema {
                return Err(Error::Schema("cannot concatenate different batch schemas".into()));
            }
            if batch.num_rows() > max_rows {
                return Err(Error::Execution("batch collection row limit exceeded".into()));
            }
            return Ok(batch);
        }
    }
    let mut total = 0usize;
    let mut columns: Vec<Vec<Value>> = (0..schema.len()).map(|_| Vec::new()).collect();
    for batch in inputs {
        if batch.schema() != schema {
            return Err(Error::Schema("cannot concatenate different batch schemas".into()));
        }
        total = total.checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("batch row count overflow".into()))?;
        if total > max_rows {
            return Err(Error::Execution("batch collection row limit exceeded".into()));
        }
        for (index, output) in columns.iter_mut().enumerate() {
            output.extend_from_slice(batch.column(index)?);
        }
    }
    Batch::new_with_row_count(schema.clone(), columns, total)
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
mod one_chunk_split_perf_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn one_chunk_preserves_nullable_rows_and_zero_width_batches() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Null], vec![Value::Integer(7)]]).unwrap();
        assert_eq!(split(&batch, 2).unwrap(), vec![batch.clone()]);
        assert_eq!(split(&batch, 100).unwrap(), vec![batch]);
        let zero = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 4).unwrap();
        assert_eq!(split(&zero, 5).unwrap(), vec![zero]);
        assert!(split(&Batch::empty(Schema::new(vec![]).unwrap()), 5).unwrap().is_empty());
    }
}
