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

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        // Skip whole windows without creating the discarded batches.
        let skip = self.size.saturating_mul(n);
        self.position = self.position.saturating_add(skip);
        self.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.num_rows().saturating_sub(self.position);
        let batches = remaining.div_ceil(self.size);
        (batches, Some(batches))
    }
}

impl ExactSizeIterator for BatchChunks<'_> {}
impl std::iter::FusedIterator for BatchChunks<'_> {}

pub fn concatenate(batches: &[Batch]) -> Result<Batch> {
    let first = batches.first().ok_or_else(|| Error::Schema("cannot concatenate without a schema".into()))?;
    // A single input is already a valid batch. Avoid allocating fresh
    // column vectors and revalidating every scalar during reconstruction.
    if batches.len() == 1 {
        return Ok(first.clone());
    }
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
    // Compatible input batches have already validated each scalar.
    Batch::from_validated_columns(schema, columns, total)
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

#[cfg(test)]
mod zero_column_concat_consolidation_tests {
    use super::*;
    use crate::schema::Schema;
    #[test]
    fn concatenation_preserves_zero_column_rows() {
        let schema = Schema::new(vec![]).unwrap();
        let a = Batch::new_with_row_count(schema.clone(), vec![], 2).unwrap();
        let b = Batch::new_with_row_count(schema, vec![], 3).unwrap();
        assert_eq!(concatenate(&[a, b]).unwrap().rows().unwrap(), vec![vec![]; 5]);
    }
}

#[cfg(test)]
mod singleton_chunk_concatenation_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn single_chunk_keeps_nullable_rows_and_zero_column_cardinality() {
        let schema = Schema::new(vec![Field::new("text", DataType::Text, true)]).unwrap();
        let input = Batch::from_rows(schema, vec![
            vec![Value::Text("é".repeat(1024))], vec![Value::Null],
        ]).unwrap();
        assert_eq!(concatenate(&[input.clone()]).unwrap(), input);
        let empty_width = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        assert_eq!(concatenate(&[empty_width.clone()]).unwrap(), empty_width);
    }
}

#[cfg(test)]
mod batch_chunk_nth_skip_tests {
    use super::*;

    #[test]
    fn jumping_over_chunks_does_not_change_remaining_rows() {
        let input = Batch::from_rows(
            crate::schema::Schema::new(vec![
                crate::schema::Field::new("n", crate::schema::DataType::Integer, false),
            ]).unwrap(),
            (0..25).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap();
        let mut iter = chunks(&input, 4).unwrap();
        assert_eq!(iter.len(), 7);
        assert_eq!(iter.nth(2).unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(8)]);
        assert_eq!(iter.len(), 4);
        assert_eq!(iter.next().unwrap().unwrap().row(0).unwrap(), vec![Value::Integer(12)]);
        assert!(iter.nth(usize::MAX).is_none());
        assert_eq!(iter.len(), 0);
        assert!(iter.next().is_none());
    }
}

#[cfg(test)]
mod trusted_concatenation_regressions {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn retains_nullable_wide_text_and_zero_column_cardinality() {
        let schema = Schema::new(vec![Field::new("payload", DataType::Text, true)]).unwrap();
        let long = Value::Text("🙂".repeat(8192));
        let first = Batch::from_rows(schema.clone(), vec![
            vec![long.clone()], vec![Value::Null],
        ]).unwrap();
        let second = Batch::from_rows(schema, vec![vec![long.clone()]]).unwrap();
        let combined = concatenate(&[first, second]).unwrap();
        assert_eq!(combined.rows().unwrap(), vec![
            vec![long.clone()], vec![Value::Null], vec![long],
        ]);
        let zero = Schema::new(vec![]).unwrap();
        let left = Batch::new_with_row_count(zero.clone(), vec![], 3).unwrap();
        let right = Batch::new_with_row_count(zero, vec![], 4).unwrap();
        assert_eq!(concatenate(&[left, right]).unwrap().num_rows(), 7);
    }
}

#[cfg(test)]
mod already_validated_chunk_concatenation_tests {
    use super::*;
    use crate::schema::{DataType, Field, Schema};

    #[test]
    fn wide_chunks_preserve_nullable_text_and_zero_width_cardinality() {
        let schema = Schema::new(vec![
            Field::new("text", DataType::Text, true),
            Field::new("id", DataType::Integer, false),
        ]).unwrap();
        let input = Batch::from_rows(schema, (0..25).map(|n| vec![
            if n % 5 == 0 { Value::Null } else { Value::Text(format!("text-{n}-{}", "🙂".repeat(500))) },
            Value::Integer(n),
        ]).collect()).unwrap();
        let chunks = chunks(&input, 4).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(concatenate(&chunks).unwrap(), input);
        let zero = Schema::new(vec![]).unwrap();
        let parts = (0..4).map(|_| Batch::new_with_row_count(zero.clone(), vec![], 3).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(concatenate(&parts).unwrap().num_rows(), 12);
        assert!(concatenate(&[chunks[0].clone(), Batch::empty(zero)]).is_err());
    }
}
