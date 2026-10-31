use crate::{
    batch::Batch,
    cancel::QueryGuard,
    expr::Expr,
    filter,
    project::{self, Selection},
    Result,
    metrics::QueryMetrics,
};

enum Step {
    Filter(Expr),
    Project(Vec<Selection>),
    // LIMIT is stateful across incoming batches and must run at its position
    // in the operator chain, not after every projection and filter.
    Limit { skip: usize, remaining: Option<usize> },
}

pub struct Pipeline<I> {
    source: I,
    steps: Vec<Step>,
    invalid_limit: bool,
    stopped: bool,
    guard: QueryGuard,
    metrics: Option<QueryMetrics>,
}

impl<I: Iterator<Item = Result<Batch>>> Pipeline<I> {
    pub fn new(source: I) -> Self {
        Self {
            source,
            steps: Vec::new(),
            invalid_limit: false,
            stopped: false,
            guard: QueryGuard::default(),
            metrics: None,
        }
    }

    pub fn with_filter(mut self, predicate: Expr) -> Self {
        self.steps.push(Step::Filter(predicate));
        self
    }

    pub fn with_projection(mut self, selections: Vec<Selection>) -> Self {
        self.steps.push(Step::Project(selections));
        self
    }

    pub fn with_limit(mut self, offset: usize, count: Option<usize>) -> Self {
        self.invalid_limit |= count.is_some_and(|rows| offset.checked_add(rows).is_none());
        self.steps.push(Step::Limit { skip: offset, remaining: count });
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
        if self.stopped { return None; }
        if self.invalid_limit {
            self.invalid_limit = false;
            self.stopped = true;
            return Some(Err(crate::Error::Execution("LIMIT + OFFSET overflow".into())));
        }
        if self.steps.iter().any(|step|
            matches!(step, Step::Limit { remaining: Some(0), .. })) {
            self.stopped = true;
            return None;
        }
        loop {
            if let Err(error) = self.guard.check() {
                self.remaining = Some(0);
                return Some(Err(error));
            }
            let Some(input) = self.source.next() else {
                // A generic source is not guaranteed to be fused: once a
                // pipeline reports EOF, never poll its source again.
                self.stopped = true;
                return None;
            };
            let result: Result<Batch> = (|| {
                let mut batch = input?;
                if let Some(metrics) = &self.metrics {
                    metrics.record_input(batch.num_rows() as u64)?;
                }
                // Respect operator order for aliases introduced by an earlier
                // projection and referenced by a later filter.
                for step in &mut self.steps {
                    batch = match step {
                        Step::Filter(predicate) => filter::filter(&batch, predicate)?,
                        Step::Project(selections) => project::project(&batch, selections)?,
                        Step::Limit { skip, remaining } => {
                            let start = (*skip).min(batch.num_rows());
                            *skip -= start;
                            let available = batch.num_rows() - start;
                            let keep = remaining.map_or(available, |left| left.min(available));
                            if let Some(left) = remaining.as_mut() { *left -= keep; }
                            if start == 0 && keep == batch.num_rows() {
                                batch
                            } else {
                                batch.slice_rows(start, start + keep)?
                            }
                        }
                    };
                }
                if let Some(metrics) = &self.metrics {
                    metrics.record_output(batch.num_rows() as u64)?;
                }
                Ok(batch)
            })();
            match result {
                Ok(batch) if batch.num_rows() == 0 => {
                    if self.steps.iter().any(|step|
                        matches!(step, Step::Limit { remaining: Some(0), .. })) {
                        self.stopped = true;
                        return None;
                    }
                    continue;
                }
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => {
                    self.stopped = true;
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
mod chained_projection_pipeline_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};

    #[test]
    fn successive_projections_can_reference_the_previous_alias() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(7)]],
        ).unwrap();
        let result = Pipeline::new([Ok(input)].into_iter())
            .with_projection(vec![Selection::new(Expr::Column("value".into()), "renamed")])
            .with_projection(vec![Selection::new(Expr::Column("renamed".into()), "last")])
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result[0].schema().fields()[0].name, "last");
        assert_eq!(result[0].row(0).unwrap(), vec![Value::Integer(7)]);
    }
}

#[cfg(test)]
mod consolidated_pipeline_regression_tests {
    use super::*;
    use crate::{expr::BinaryOp, scalar::Value, schema::{DataType, Field, Schema}};
    fn sample() -> Batch {
        Batch::from_rows(Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (0..10).map(|n| vec![Value::Integer(n)]).collect()).unwrap()
    }
    #[test]
    fn chained_filters_are_intersected() {
        let greater = Expr::binary(Expr::Column("n".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(1)));
        let smaller = Expr::binary(Expr::Column("n".into()), BinaryOp::Less,
            Expr::Literal(Value::Integer(4)));
        let batches = Pipeline::new([Ok(sample())].into_iter())
            .with_filter(greater).with_filter(smaller).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches[0].rows().unwrap(), vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
    }
    #[test]
    fn successive_limits_compose_in_order() {
        let results = Pipeline::new([Ok(sample())].into_iter())
            .with_limit(2, Some(5)).with_limit(1, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results[0].rows().unwrap(), vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
        let none = Pipeline::new([Ok(sample())].into_iter())
            .with_limit(1, Some(2)).with_limit(9, Some(1))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(none.iter().map(Batch::num_rows).sum::<usize>(), 0);
    }
    #[test]
    fn limit_overflow_errors_before_polling_input() {
        let result = Pipeline::new([Ok(sample())].into_iter())
            .with_limit(usize::MAX, Some(2)).next().unwrap();
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod fused_pipeline_eof_tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[test]
    fn pipeline_does_not_poll_a_nonfused_source_after_eof() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let source = std::iter::from_fn(move || {
            let n = observed.fetch_add(1, Ordering::Relaxed);
            if n == 0 { None::<Result<Batch>> }
            else { panic!("polled non-fused source after EOF") }
        });
        let mut pipeline = Pipeline::new(source);
        assert!(pipeline.next().is_none());
        assert!(pipeline.next().is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod pipeline_operator_order_tests {
    use super::*;
    use crate::{
        expr::BinaryOp,
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn filter_after_projection_can_use_its_alias() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap(),
            (0..5).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap();
        let projected = vec![Selection::new(Expr::Column("value".into()), "renamed")];
        let filter = Expr::binary(
            Expr::Column("renamed".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(2)),
        );
        let output = Pipeline::new([Ok(input)].into_iter())
            .with_projection(projected)
            .with_filter(filter)
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output[0].rows().unwrap(), vec![
            vec![Value::Integer(3)], vec![Value::Integer(4)],
        ]);
    }

    #[test]
    fn intervening_filter_does_not_jump_ahead_of_projection() {
        let input = Batch::from_rows(
            Schema::new(vec![Field::new("x", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap();
        let output = Pipeline::new([Ok(input)].into_iter())
            .with_projection(vec![Selection::new(Expr::Column("x".into()), "y")])
            .with_filter(Expr::Literal(Value::Boolean(true)))
            .with_projection(vec![Selection::new(Expr::Column("y".into()), "z")])
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output[0].schema().fields()[0].name, "z");
    }
}

#[cfg(test)]
mod limit_operator_order_regressions {
    use super::*;
    use crate::{expr::BinaryOp, scalar::Value, schema::{DataType, Field, Schema}};
    use std::{cell::Cell, rc::Rc};

    fn batch(start: i64) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            (start..start + 4).map(|n| vec![Value::Integer(n)]).collect(),
        ).unwrap()
    }

    #[test]
    fn limit_before_filter_must_not_read_later_batches() {
        let polls = Rc::new(Cell::new(0));
        let seen = Rc::clone(&polls);
        let source = std::iter::repeat_with(move || {
            let index = seen.get();
            seen.set(index + 1);
            Ok(batch(index as i64 * 4))
        });
        let predicate = Expr::binary(Expr::Column("n".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(10)));
        let output = Pipeline::new(source).with_limit(0, Some(3))
            .with_filter(predicate).collect::<Result<Vec<_>>>().unwrap();
        assert!(output.is_empty());
        assert_eq!(polls.get(), 1, "upstream LIMIT must stop source polling");
    }

    #[test]
    fn filter_before_limit_can_collect_from_subsequent_batches() {
        let predicate = Expr::binary(Expr::Column("n".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(4)));
        let results = Pipeline::new([Ok(batch(0)), Ok(batch(4)), Ok(batch(8))].into_iter())
            .with_filter(predicate).with_limit(1, Some(3))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results.into_iter().flat_map(|batch| batch.rows().unwrap())
            .collect::<Vec<_>>(), vec![
                vec![Value::Integer(6)], vec![Value::Integer(7)],
                vec![Value::Integer(8)],
            ]);
    }

    #[test]
    fn interleaved_limits_and_alias_filters_preserve_order() {
        let predicate = Expr::binary(Expr::Column("renamed".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(2)));
        let output = Pipeline::new([Ok(batch(0)), Ok(batch(4))].into_iter())
            .with_limit(1, Some(5))
            .with_projection(vec![Selection::new(Expr::Column("n".into()), "renamed")])
            .with_filter(predicate)
            .with_limit(1, Some(2))
            .collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(output.into_iter().flat_map(|part| part.rows().unwrap())
            .collect::<Vec<_>>(), vec![
                vec![Value::Integer(4)], vec![Value::Integer(5)]
            ]);
    }
}
