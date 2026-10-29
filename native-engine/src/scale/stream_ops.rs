//! Fallible pipelined operators over bounded columnar batches.
use crate::{batch::Batch, expr::Expr, filter, project::{self, Selection}, Result};
use super::cancel::QueryControl;

pub struct FilterStream<I> {
    input: I,
    predicate: Expr,
    control: QueryControl,
    stopped: bool,
}

impl<I> FilterStream<I> {
    pub fn new(input: I, predicate: Expr, control: QueryControl) -> Self {
        Self { input, predicate, control, stopped: false }
    }
}

impl<I: Iterator<Item = Result<Batch>>> Iterator for FilterStream<I> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let next = self.input.next()?;
            let result = next.and_then(|batch| filter::filter(&batch, &self.predicate));
            match result {
                Ok(batch) if batch.num_rows() == 0 => continue,
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

pub struct ProjectStream<I> {
    input: I,
    selections: Vec<Selection>,
    control: QueryControl,
    stopped: bool,
}

impl<I> ProjectStream<I> {
    pub fn new(input: I, selections: Vec<Selection>, control: QueryControl) -> Self {
        Self { input, selections, control, stopped: false }
    }
}

impl<I: Iterator<Item = Result<Batch>>> Iterator for ProjectStream<I> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        if let Err(error) = self.control.check() {
            self.stopped = true;
            return Some(Err(error));
        }
        let next = self.input.next()?;
        let result = next.and_then(|batch| project::project(&batch, &self.selections));
        if result.is_err() { self.stopped = true; }
        Some(result)
    }
}

pub struct LimitStream<I> {
    input: I,
    offset: usize,
    remaining: Option<usize>,
    control: QueryControl,
    stopped: bool,
}

impl<I> LimitStream<I> {
    pub fn new(input: I, offset: usize, count: Option<usize>, control: QueryControl) -> Self {
        Self { input, offset, remaining: count, control, stopped: false }
    }
}

impl<I: Iterator<Item = Result<Batch>>> Iterator for LimitStream<I> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped || self.remaining == Some(0) { return None; }
        loop {
            if let Err(error) = self.control.check() {
                self.stopped = true;
                return Some(Err(error));
            }
            let next = self.input.next()?;
            let batch = match next {
                Ok(batch) => batch,
                Err(error) => {
                    self.stopped = true;
                    return Some(Err(error));
                }
            };
            let skipped = self.offset.min(batch.num_rows());
            self.offset -= skipped;
            let available = batch.num_rows() - skipped;
            let count = self.remaining.map_or(available, |left| left.min(available));
            if count == 0 { continue; }
            if let Some(remaining) = self.remaining.as_mut() { *remaining -= count; }
            let result = batch.take(&(skipped..skipped + count).collect::<Vec<_>>());
            if result.is_err() { self.stopped = true; }
            return Some(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scale::chunks::RowBatcher, expr::BinaryOp, scalar::Value,
        schema::{DataType, Field, Schema}};

    #[test]
    fn filters_project_and_limit_across_batch_boundaries() {
        let schema = Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap();
        let source = RowBatcher::new((0..17).map(|i| Ok(vec![Value::Integer(i)])), schema, 3).unwrap();
        let filtered = FilterStream::new(
            source, Expr::binary(Expr::Column("x".into()), BinaryOp::Greater,
                Expr::Literal(Value::Integer(7))), QueryControl::default());
        let projected = ProjectStream::new(filtered, vec![
            Selection::new(Expr::Column("x".into()), "value")
        ], QueryControl::default());
        let limited = LimitStream::new(projected, 2, Some(4), QueryControl::default());
        let all = limited.collect::<Result<Vec<_>>>().unwrap();
        let rows = all.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![
            vec![Value::Integer(10)], vec![Value::Integer(11)],
            vec![Value::Integer(12)], vec![Value::Integer(13)]
        ]);
    }
}

#[cfg(test)]
mod cancelled_io_tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};
    use crate::{Error, scalar::Value};
    #[test]
    fn pre_cancelled_filter_does_not_read_upstream() {
        let reads = Rc::new(Cell::new(0));
        let calls = Rc::clone(&reads);
        let source = std::iter::once_with(move || {
            calls.set(calls.get() + 1);
            Err::<Batch, Error>(Error::Execution("unexpected read".into()))
        });
        let control = QueryControl::default();
        control.cancel();
        let mut stream = FilterStream::new(source, Expr::Literal(Value::Boolean(true)), control);
        assert!(stream.next().unwrap().unwrap_err().to_string().contains("cancelled"));
        assert!(stream.next().is_none());
        assert_eq!(reads.get(), 0);
    }
}
