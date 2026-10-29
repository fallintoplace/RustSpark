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

        let mut rows = Vec::with_capacity(self.batch_rows.min(65_536));
        while rows.len() < self.batch_rows.min(65_536) {
            match self.input.next() {
                Some(Ok(row)) => rows.push(row),
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
        let end = self.position.saturating_add(self.batch_rows).min(self.input.num_rows());
        let indices = (self.position..end).collect::<Vec<_>>();
        self.position = end;
        Some(self.input.take(&indices))
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
mod max_batch_budget_tests {
    use super::*;
    use crate::schema::{DataType, Field};
    #[test]
    fn large_requested_batch_is_split_at_memory_boundary() {
        let schema = Schema::new(vec![Field::new("v", DataType::Integer, false)]).unwrap();
        let rows = (0..65_537).map(|n| Ok(vec![Value::Integer(n)]));
        let batches = RowBatcher::new(rows, schema, usize::MAX).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.iter().map(Batch::num_rows).collect::<Vec<_>>(), [65_536, 1]);
    }
}
