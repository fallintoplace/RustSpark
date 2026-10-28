//! Streaming filter/project/limit pipeline with cancellation checkpoints.
use crate::{
    batch::Batch,
    cancellation::QueryCancellation,
    expr::Expr,
    filter, limit, project::{self, Selection},
    Error, Result,
};

pub struct Pipeline<I> {
    input: I,
    cancellation: QueryCancellation,
    predicate: Option<Expr>,
    projection: Option<Vec<Selection>>,
    remaining: Option<usize>,
    finished: bool,
}

pub fn pipeline<I>(
    input: I,
    cancellation: QueryCancellation,
    predicate: Option<Expr>,
    projection: Option<Vec<Selection>>,
    max_rows: Option<usize>,
) -> Pipeline<I::IntoIter>
where
    I: IntoIterator<Item = Result<Batch>>,
{
    Pipeline {
        input: input.into_iter(),
        cancellation,
        predicate,
        projection,
        remaining: max_rows,
        finished: false,
    }
}

impl<I> Iterator for Pipeline<I>
where
    I: Iterator<Item = Result<Batch>>,
{
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.remaining == Some(0) { return None; }
        loop {
            if let Err(error) = self.cancellation.checkpoint() {
                self.finished = true;
                return Some(Err(error));
            }
            let batch = match self.input.next() {
                Some(Ok(batch)) => batch,
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(error));
                }
                None => {
                    self.finished = true;
                    return None;
                }
            };
            let processed = (|| {
                let filtered = match &self.predicate {
                    Some(predicate) => filter::filter(&batch, predicate)?,
                    None => batch,
                };
                let projected = match &self.projection {
                    Some(selections) if selections.is_empty() =>
                        return Err(Error::Unsupported("empty projection".into())),
                    Some(selections) => project::project(&filtered, selections)?,
                    None => filtered,
                };
                match self.remaining {
                    Some(rows) => limit::limit(&projected, 0, Some(rows)),
                    None => Ok(projected),
                }
            })();
            match processed {
                Ok(batch) => {
                    if let Some(remaining) = &mut self.remaining {
                        *remaining -= batch.num_rows();
                    }
                    if batch.num_rows() == 0 {
                        if self.remaining == Some(0) {
                            self.finished = true;
                            return None;
                        }
                        continue;
                    }
                    return Some(Ok(batch));
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        expr::{BinaryOp, Expr},
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    fn source(values: &[i64]) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap(),
            values.iter().map(|n| vec![Value::Integer(*n)]).collect(),
        ).unwrap()
    }

    #[test]
    fn filters_projects_and_stops_early_across_batches() {
        let guard = QueryCancellation::new();
        let predicate = Expr::binary(
            Expr::Column("id".into()), BinaryOp::Greater,
            Expr::Literal(Value::Integer(2)),
        );
        let selections = vec![Selection::new(Expr::Column("id".into()), "result")];
        let output = pipeline(
            vec![Ok(source(&[1, 2, 3])), Ok(source(&[4, 5, 6]))],
            guard,
            Some(predicate),
            Some(selections),
            Some(2),
        ).collect::<Result<Vec<_>>>().unwrap();
        let rows = output.into_iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(3)], vec![Value::Integer(4)]]);
    }

    #[test]
    fn cancellation_stops_pipeline_before_processing_input() {
        let guard = QueryCancellation::new();
        let token = guard.clone();
        let mut stream = pipeline(vec![Ok(source(&[1]))], guard, None, None, None);
        token.cancel();
        assert!(stream.next().unwrap().is_err());
        assert!(stream.next().is_none());
    }
}
