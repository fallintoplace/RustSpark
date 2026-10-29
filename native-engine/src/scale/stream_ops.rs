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
            return Some(batch.take(&(skipped..skipped + count).collect::<Vec<_>>()));
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
mod cancellation_before_upstream_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::{cell::Cell, rc::Rc};

    #[test]
    fn streaming_operators_never_poll_cancelled_inputs() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap();
        let control = QueryControl::default();
        control.cancel();
        let polls = Rc::new(Cell::new(0));
        let source = || {
            let polls = Rc::clone(&polls);
            std::iter::once(Ok(input.clone())).inspect(move |_| polls.set(polls.get() + 1))
        };
        let mut filter = FilterStream::new(source(),
            Expr::Literal(Value::Boolean(true)), control.clone());
        assert!(filter.next().unwrap().is_err());
        assert!(filter.next().is_none());
        let mut project = ProjectStream::new(source(), vec![], control.clone());
        assert!(project.next().unwrap().is_err());
        assert!(project.next().is_none());
        let mut limit = LimitStream::new(source(), 0, Some(1), control);
        assert!(limit.next().unwrap().is_err());
        assert!(limit.next().is_none());
        assert_eq!(polls.get(), 0);
    }
}
