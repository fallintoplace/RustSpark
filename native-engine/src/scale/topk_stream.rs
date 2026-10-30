//! Native bounded Top-K for ORDER BY with LIMIT and OFFSET.
//! Keeps O(offset + limit) candidate rows instead of spilling the full input.
use std::{cmp::Ordering, collections::{BinaryHeap, VecDeque}, sync::Arc};

use crate::{
    batch::Batch, scalar::Value, schema::Schema, sort::{self, SortKey}, Error, Result,
};
use super::{cancel::QueryControl, stream_execute::BatchStream};

#[derive(Clone, Debug)]
struct Candidate {
    keys: Vec<Value>,
    row: Vec<Value>,
    ordinal: usize,
    options: Arc<Vec<(bool, bool)>>,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        for ((lhs, rhs), (descending, nulls_first)) in self.keys.iter()
            .zip(&other.keys).zip(self.options.iter()) {
            let order = sort::compare_values(lhs, rhs, *descending, *nulls_first);
            if order != Ordering::Equal { return order; }
        }
        self.ordinal.cmp(&other.ordinal)
    }
}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}
impl Eq for Candidate {}

pub struct TopKOutput {
    batches: VecDeque<Batch>,
    control: QueryControl,
    stopped: bool,
}

impl TopKOutput {
    pub fn new(
        source: BatchStream, schema: Schema, keys: &[SortKey],
        offset: usize, count: usize, batch_rows: usize, control: QueryControl,
    ) -> Result<Self> {
        if batch_rows == 0 || batch_rows > 65_536 {
            return Err(Error::Execution("invalid Top-K output batch size".into()));
        }
        let target = offset.checked_add(count)
            .filter(|rows| *rows <= 100_000)
            .ok_or_else(|| Error::Execution("stream Top-K candidate budget exceeded".into()))?;
        for key in keys { key.expression.output_type(&schema)?; }
        let options = Arc::new(keys.iter().map(|key|
            (key.descending, key.nulls_first)).collect::<Vec<_>>());
        let mut heap = BinaryHeap::<Candidate>::with_capacity(target);
        let mut ordinal = 0usize;
        if target > 0 {
            for next in source {
                control.check()?;
                let batch = next?;
                if batch.schema() != &schema {
                    return Err(Error::Schema("native Top-K input schema changed".into()));
                }
                for row in 0..batch.num_rows() {
                    if row % 1024 == 0 { control.check()?; }
                    let keys = keys.iter().map(|key| key.expression.evaluate(&batch, row))
                        .collect::<Result<Vec<_>>>()?;
                    let candidate = Candidate {
                        keys, row: Vec::new(), ordinal, options: Arc::clone(&options),
                    };
                    ordinal = ordinal.checked_add(1)
                        .ok_or_else(|| Error::Execution("stream Top-K ordinal overflow".into()))?;
                    if heap.len() < target {
                        let mut candidate = candidate;
                        candidate.row = batch.row(row)?;
                        heap.push(candidate);
                    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                        let mut candidate = candidate;
                        candidate.row = batch.row(row)?;
                        *heap.peek_mut().expect("nonempty Top-K heap") = candidate;
                    }
                }
            }
        }
        let mut batches = VecDeque::new();
        let mut rows = Vec::with_capacity(count.min(batch_rows));
        for candidate in heap.into_sorted_vec().into_iter().skip(offset).take(count) {
            rows.push(candidate.row);
            if rows.len() == batch_rows {
                batches.push_back(Batch::from_rows(schema.clone(), std::mem::take(&mut rows))?);
            }
        }
        if !rows.is_empty() {
            batches.push_back(Batch::from_rows(schema, rows)?);
        }
        Ok(Self { batches, control, stopped: false })
    }
}

impl Iterator for TopKOutput {
    type Item = Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped { return None; }
        if let Err(error) = self.control.check() {
            self.batches.clear();
            self.stopped = true;
            return Some(Err(error));
        }
        let result = self.batches.pop_front();
        if result.is_none() { self.stopped = true; }
        result.map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, schema::{DataType, Field}, scalar::Value};

    fn schema() -> Schema {
        Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap()
    }

    #[test]
    fn bounded_heap_matches_native_topk_across_input_batches() {
        let schema = schema();
        let keys = [SortKey::ascending(Expr::Column("n".into()))];
        let values = [10, 1, 2, 3, 2, 4];
        let first = Batch::from_rows(schema.clone(), values[..3].iter()
            .map(|n| vec![Value::Integer(*n)]).collect()).unwrap();
        let second = Batch::from_rows(schema.clone(), values[3..].iter()
            .map(|n| vec![Value::Integer(*n)]).collect()).unwrap();
        let all = Batch::from_rows(schema.clone(), values.iter()
            .map(|n| vec![Value::Integer(*n)]).collect()).unwrap();
        let result = TopKOutput::new(Box::new([Ok(first), Ok(second)].into_iter()),
            schema, &keys, 1, 3, 2, QueryControl::default()).unwrap()
            .collect::<Result<Vec<_>>>().unwrap();
        assert!(result.iter().all(|batch| batch.num_rows() <= 2));
        let rows = result.iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, crate::topk::top_k_offset(&all, &keys, 1, 3).unwrap()
            .rows().unwrap());
    }

    #[test]
    fn supports_computed_sort_keys_and_rejects_invalid_expressions() {
        let key = SortKey::ascending(Expr::binary(Expr::Column("n".into()),
            BinaryOp::Multiply, Expr::Literal(Value::Integer(-1))));
        let input = Batch::from_rows(schema(), vec![
            vec![Value::Integer(1)], vec![Value::Integer(4)],
            vec![Value::Integer(3)],
        ]).unwrap();
        let rows = TopKOutput::new(Box::new([Ok(input)].into_iter()),
            schema(), &[key], 0, 2, 1, QueryControl::default()).unwrap()
            .collect::<Result<Vec<_>>>().unwrap()
            .iter().flat_map(|batch| batch.rows().unwrap()).collect::<Vec<_>>();
        assert_eq!(rows, vec![vec![Value::Integer(4)], vec![Value::Integer(3)]]);
        let bad = [SortKey::ascending(Expr::Column("missing".into()))];
        assert!(TopKOutput::new(Box::new(std::iter::empty()), schema(),
            &bad, 0, 0, 10, QueryControl::default()).is_err());
    }
}
