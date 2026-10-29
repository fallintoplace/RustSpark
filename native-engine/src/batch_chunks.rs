//! Batch chunking and reconstruction for streaming physical operators.
use crate::{batch::Batch, scalar::Value, Error, Result};

pub struct BatchChunks<'a> {
    batch: &'a Batch,
    size: usize,
    position: usize,
}

pub fn chunks(batch: &Batch, size: usize) -> Result<BatchChunks<'_>> {
    if size == 0 {
        return Err(Error::Execution("batch chunk size must be positive".into()));
    }
    Ok(BatchChunks { batch, size, position: 0 })
}

impl Iterator for BatchChunks<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.batch.num_rows() {
            return None;
        }
        let start = self.position;
        let end = start.saturating_add(self.size).min(self.batch.num_rows());
        self.position = end;
        Some(self.batch.slice_rows(start, end))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.num_rows().saturating_sub(self.position);
        let batches = remaining / self.size + usize::from(remaining % self.size != 0);
        (batches, Some(batches))
    }
}

pub fn concatenate(batches: &[Batch]) -> Result<Batch> {
    let first = batches.first().ok_or_else(|| Error::Schema("cannot concatenate without a schema".into()))?;
    let schema = first.schema().clone();
    let mut total = 0_usize;
    for batch in batches {
        if batch.schema() != &schema {
            return Err(Error::Schema("batch schemas must match exactly".into()));
        }
        total = total.checked_add(batch.num_rows())
            .ok_or_else(|| Error::Execution("batch row count overflow".into()))?;
    }
    let mut columns: Vec<Vec<Value>> = (0..schema.len())
        .map(|_| Vec::with_capacity(total))
        .collect();
    for batch in batches {
        for (index, column) in columns.iter_mut().enumerate() {
            column.extend_from_slice(batch.column(index)?);
        }
    }
    Batch::new(schema, columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    fn make_batch() -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            (0..9).map(|n| vec![Value::Integer(n)]).collect()
        ).unwrap()
    }

    #[test]
    fn chunk_sizes_and_rows_remain_stable() {
        let original = make_batch();
        let parts = chunks(&original, 4).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![4, 4, 1]);
        assert_eq!(concatenate(&parts).unwrap(), original);
        assert!(chunks(&original, 0).is_err());
    }

    #[test]
    fn rejects_incompatible_schemas() {
        let first = make_batch();
        let other = Batch::empty(Schema::new(vec![Field::new("another", DataType::Text, true)]).unwrap());
        assert!(concatenate(&[first, other]).is_err());
        assert!(concatenate(&[]).is_err());
    }
}
