use crate::{batch::Batch, schema::Schema, Error, Result};

pub fn split(batch: &Batch, rows_per_batch: usize) -> Result<Vec<Batch>> {
    if rows_per_batch == 0 {
        return Err(Error::Execution("batch size must be positive".into()));
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < batch.num_rows() {
        let end = start.saturating_add(rows_per_batch).min(batch.num_rows());
        chunks.push(batch.take(&(start..end).collect::<Vec<_>>())?);
        start = end;
    }
    Ok(chunks)
}

pub fn concat(
    schema: &Schema,
    batches: impl IntoIterator<Item = Batch>,
    max_rows: usize,
) -> Result<Batch> {
    let mut total = 0usize;
    let mut columns = vec![Vec::new(); schema.len()];
    for batch in batches {
        if batch.schema() != schema {
            return Err(Error::Schema("cannot concatenate different batch schemas".into()));
        }
        total = total.checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("batch row count overflow".into()))?;
        if total > max_rows {
            return Err(Error::Execution("batch collection row limit exceeded".into()));
        }
        for (index, column) in columns.iter_mut().enumerate() {
            column.extend_from_slice(batch.column(index)?);
        }
    }
    Batch::new(schema.clone(), columns)
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
    fn concatenate_rejects_schema_changes() {
        let a = Schema::new(vec![Field::new("a", DataType::Text, true)]).unwrap();
        let b = Schema::new(vec![Field::new("b", DataType::Text, true)]).unwrap();
        assert!(concat(&a, [Batch::empty(b)], 10).is_err());
    }
}

#[cfg(test)]
mod columnar_concatenation_tests {
    use super::*;
    use crate::{schema::{DataType, Field}, scalar::Value};

    #[test]
    fn empty_batches_keep_schema_and_nullable_values() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Integer, true),
            Field::new("text", DataType::Text, true),
        ]).unwrap();
        let empty = Batch::empty(schema.clone());
        let row = Batch::from_rows(schema.clone(),
            vec![vec![Value::Null, Value::Text("ok".into())]]).unwrap();
        let merged = concat(&schema, [empty, row], 1).unwrap();
        assert_eq!(merged.row(0).unwrap(), vec![Value::Null, Value::Text("ok".into())]);
        assert_eq!(concat(&schema, Vec::<Batch>::new(), 0).unwrap().num_rows(), 0);
    }
}
