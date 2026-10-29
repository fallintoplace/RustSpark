//! Convert fallible row streams into independently validated bounded batches.
use crate::{batch::Batch, scalar::Value, schema::Schema, Error, Result};

pub struct RowBatcher<I> {
    input: I,
    schema: Schema,
    batch_rows: usize,
    pending_error: Option<Error>,
    finished: bool,
}

impl<I> RowBatcher<I>
where I: Iterator<Item = Result<Vec<Value>>>
{
    pub fn new(input: I, schema: Schema, batch_rows: usize) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::Execution("batch size must be positive".into()));
        }
        Ok(Self { input, schema, batch_rows, pending_error: None, finished: false })
    }
}

impl<I> Iterator for RowBatcher<I>
where I: Iterator<Item = Result<Vec<Value>>>
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished { return None; }
        if let Some(error) = self.pending_error.take() {
            self.finished = true;
            return Some(Err(error));
        }

        let limit = self.batch_rows.min(65_536);
        let capacity = self.input.size_hint().1.map_or(limit, |upper| upper.min(limit));
        let mut rows = Vec::with_capacity(capacity);
        while rows.len() < limit {
            match self.input.next() {
                Some(Ok(row)) => {
                    if row.len() != self.schema.len() {
                        let error = Error::Schema("row width does not match schema".into());
                        if rows.is_empty() {
                            self.finished = true;
                            return Some(Err(error));
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                    rows.push(row);
                },
                Some(Err(error)) => {
                    if rows.is_empty() {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.pending_error = Some(error);
                    break;
                }
                None => {
                    self.finished = true;
                    break;
                }
            }
        }
        if rows.is_empty() { return None; }
        let batch = Batch::from_rows(self.schema.clone(), rows);
        if batch.is_err() { self.finished = true; }
        Some(batch)
    }
}

pub struct BatchChunks<'a> {
    input: &'a Batch,
    position: usize,
    batch_rows: usize,
}

impl<'a> BatchChunks<'a> {
    pub fn new(input: &'a Batch, batch_rows: usize) -> Result<Self> {
        if batch_rows == 0 {
            return Err(Error::Execution("batch size must be positive".into()));
        }
        Ok(Self { input, position: 0, batch_rows })
    }
}

impl Iterator for BatchChunks<'_> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.input.num_rows() { return None; }
        let start = self.position;
        let end = start.saturating_add(self.batch_rows).min(self.input.num_rows());
        self.position = end;
        Some(self.input.slice_rows(start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DataType, Field};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap()
    }

    #[test]
    fn row_batched_input_never_exceeds_target_size() {
        let rows = (0..1005).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(rows, schema(), 128).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).sum::<usize>(), 1005);
        assert!(batches.iter().all(|b| b.num_rows() <= 128));
        assert_eq!(batches.last().unwrap().row(108).unwrap(), vec![Value::Integer(1004)]);
    }

    #[test]
    fn buffered_rows_are_not_lost_before_errors() {
        let input = vec![Ok(vec![Value::Integer(7)]), Err(Error::Parse("bad row".into()))];
        let mut batches = RowBatcher::new(input.into_iter(), schema(), 4).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().num_rows(), 1);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }
}

#[cfg(test)]
mod batcher_malformed_width_tests {
    use super::*;

    #[test]
    fn returns_valid_prefix_then_terminal_width_error() {
        let schema = Schema::new(vec![
            crate::schema::Field::new("n", crate::schema::DataType::Integer, false)
        ]).unwrap();
        let input = vec![
            Ok(vec![Value::Integer(5)]),
            Ok(vec![]),
            Ok(vec![Value::Integer(10)]),
        ];
        let mut batches = RowBatcher::new(input.into_iter(), schema, 8).unwrap();
        assert_eq!(batches.next().unwrap().unwrap().rows().unwrap(),
            vec![vec![Value::Integer(5)]]);
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
    }
}

#[cfg(test)]
mod consolidated_bounded_row_batch_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn huge_requested_batch_stays_bounded() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let rows = (0..65_537).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(rows, schema, usize::MAX)
            .unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), vec![65_536, 1]);
    }
}

#[cfg(test)]
mod scale_contiguous_chunk_perf_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn slice_iterator_preserves_nullable_data_and_zero_width_counts() {
        let source = Batch::from_rows(Schema::new(vec![Field::new("x", DataType::Text, true)]).unwrap(),
            vec![vec![Value::Null], vec![Value::Text("one".into())], vec![Value::Null]]).unwrap();
        let parts = BatchChunks::new(&source, 2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 1]);
        assert_eq!(parts.iter().flat_map(|part| part.rows().unwrap()).collect::<Vec<_>>(), source.rows().unwrap());

        let zero_width = Batch::new_with_row_count(Schema::new(vec![]).unwrap(), vec![], 5).unwrap();
        let parts = BatchChunks::new(&zero_width, 2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(parts.iter().map(Batch::num_rows).collect::<Vec<_>>(), [2, 2, 1]);
    }
}

#[cfg(test)]
mod bounded_remaining_rows_capacity_tests {
    use super::*;
    #[test]
    fn short_iterators_are_collected_with_oversized_batch_targets() {
        let schema = Schema::new(vec![crate::schema::Field::new(
            "n", crate::schema::DataType::Integer, false)]).unwrap();
        let input = (0..3).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(input, schema, 65_536).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows().unwrap(), vec![
            vec![Value::Integer(0)], vec![Value::Integer(1)], vec![Value::Integer(2)]
        ]);
    }
}
