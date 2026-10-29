use crate::{
    batch::Batch,
    cancel::QueryGuard,
    expr::Expr,
    filter, limit,
    project::{self, Selection},
    Result,
    metrics::QueryMetrics,
};

pub struct Pipeline<I> {
    source: I,
    predicate: Option<Expr>,
    selections: Option<Vec<Selection>>,
    skip: usize,
    remaining: Option<usize>,
    guard: QueryGuard,
    metrics: Option<QueryMetrics>,
}

impl<I: Iterator<Item = Result<Batch>>> Pipeline<I> {
    pub fn new(source: I) -> Self {
        Self {
            source,
            predicate: None,
            selections: None,
            skip: 0,
            remaining: None,
            guard: QueryGuard::default(),
            metrics: None,
        }
    }

    pub fn with_filter(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    pub fn with_projection(mut self, selections: Vec<Selection>) -> Self {
        self.selections = Some(selections);
        self
    }

    pub fn with_limit(mut self, offset: usize, count: Option<usize>) -> Self {
        let available = self.remaining.map(|previous| previous.saturating_sub(offset));
        let advance = offset.min(self.remaining.unwrap_or(usize::MAX));
        self.skip = self.skip.saturating_add(advance);
        self.remaining = match (available, count) {
            (Some(previous), Some(current)) => Some(previous.min(current)),
            (Some(previous), None) => Some(previous),
            (None, current) => current,
        };
        self
    }

    pub fn with_metrics(mut self, metrics: QueryMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_guard(mut self, guard: QueryGuard) -> Self {
        self.guard = guard;
        self
    }
}

impl<I: Iterator<Item = Result<Batch>>> Iterator for Pipeline<I> {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == Some(0) {
            return None;
        }
        loop {
            if let Err(error) = self.guard.check() {
                self.remaining = Some(0);
                return Some(Err(error));
            }
            let input = self.source.next()?;
            let result: Result<Batch> = (|| {
                let mut batch = input?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_input(batch.num_rows() as u64)?;
                }
                if let Some(predicate) = &self.predicate {
                    batch = filter::filter(&batch, predicate)?;
                }
                if let Some(selections) = &self.selections {
                    batch = project::project(&batch, selections)?;
                }
                if self.skip > 0 {
                    let skipped = self.skip.min(batch.num_rows());
                    self.skip -= skipped;
                    batch = limit::limit(&batch, skipped, None)?;
                }
                if let Some(remaining) = self.remaining {
                    let taking = remaining.min(batch.num_rows());
                    batch = limit::limit(&batch, 0, Some(taking))?;
                    self.remaining = Some(remaining - taking);
                }
                if let Some(metrics) = &self.metrics {
                    metrics.record_output(batch.num_rows() as u64)?;
                }
                Ok(batch)
            })();
            match result {
                Ok(batch) if batch.num_rows() == 0 && self.remaining != Some(0) => continue,
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => {
                    self.remaining = Some(0);
                    return Some(Err(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};
    use crate::{
        expr::BinaryOp,
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    fn input() -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap(),
            (0..4).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap()
    }

    #[test]
    fn filters_and_projects_before_applying_global_limit() {
        let predicate = Expr::binary(
            Expr::Column("value".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(1)),
        );
        let source = vec![Ok(input()), Ok(input())].into_iter();
        let output = Pipeline::new(source)
            .with_filter(predicate)
            .with_projection(vec![Selection::new(Expr::Column("value".into()), "x")])
            .with_limit(1, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).sum::<usize>(), 2);
        assert_eq!(output[0].row(0).unwrap(), vec![Value::Integer(3)]);
        assert_eq!(output[1].row(0).unwrap(), vec![Value::Integer(2)]);
    }

    #[test]
    fn limit_stops_reading_upstream() {
        let calls = Rc::new(Cell::new(0));
        let calls_inner = Rc::clone(&calls);
        let source = std::iter::repeat_with(move || {
            calls_inner.set(calls_inner.get() + 1);
            Ok(input())
        });
        let results = Pipeline::new(source).with_limit(0, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results[0].num_rows(), 2);
        assert_eq!(calls.get(), 1);
    }
}

#[cfg(test)]
mod composed_pagination_window_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn nested_limit_offset_windows_keep_earlier_boundaries() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..10).map(|n| vec![Value::Integer(n)]).collect()
        ).unwrap();
        let result = Pipeline::new([Ok(batch.clone())].into_iter())
            .with_limit(2, Some(5)).with_limit(1, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result[0].rows().unwrap(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(4)]
        ]);
        let empty = Pipeline::new([Ok(batch)].into_iter())
            .with_limit(0, Some(2)).with_limit(9, Some(3))
            .collect::<Result<Vec<_>>>().unwrap();
        assert!(empty.is_empty());
    }
}
