//! Bounded worker execution for independent native partitions.
use std::{collections::VecDeque, panic::{AssertUnwindSafe, catch_unwind}, sync::Mutex};
use crate::{batch::Batch, Error, Result};

/// Execute a partition-local physical operator across at most max_workers
/// OS threads. Results are emitted in input order irrespective of completion order.
pub fn map_partitions<F>(input: Vec<Batch>, max_workers: usize, operator: F) -> Result<Vec<Batch>>
where
    F: Fn(Batch) -> Result<Batch> + Sync,
{
    if max_workers == 0 {
        return Err(Error::Execution("parallel worker count must be positive".into()));
    }
    let count = input.len();
    if count == 0 { return Ok(Vec::new()); }
    let tasks = Mutex::new(input.into_iter().enumerate().collect::<VecDeque<_>>());
    let output: Mutex<Vec<Option<Result<Batch>>>> =
        Mutex::new(std::iter::repeat_with(|| None).take(count).collect());
    std::thread::scope(|scope| {
        for _ in 0..max_workers.min(count) {
            let tasks = &tasks;
            let output = &output;
            let operator = &operator;
            scope.spawn(move || loop {
                let next = tasks.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
                let Some((index, batch)) = next else { break; };
                let result = catch_unwind(AssertUnwindSafe(|| operator(batch)))
                    .unwrap_or_else(|_| Err(Error::Execution("native worker panicked".into())));
                output.lock().unwrap_or_else(|e| e.into_inner())[index] = Some(result);
            });
        }
    });
    output.into_inner().unwrap_or_else(|e| e.into_inner())
        .into_iter()
        .map(|result| result.unwrap_or_else(|| Err(Error::Execution("missing partition result".into()))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    fn batch(n: i64) -> Batch {
        Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(n)]],
        ).unwrap()
    }

    #[test]
    fn maintains_partition_order_with_parallel_workers() {
        let input = (0..100).map(batch).collect();
        let output = map_partitions(input, 4, |batch| Ok(batch)).unwrap();
        for (index, partition) in output.iter().enumerate() {
            assert_eq!(partition.row(0).unwrap()[0], Value::Integer(index as i64));
        }
        assert!(map_partitions(Vec::new(), 0, Ok).is_err());
    }

    #[test]
    fn propagates_operator_errors_and_panics() {
        assert!(map_partitions(vec![batch(1)], 1, |_| Err(Error::Execution("bad".into()))).is_err());
        assert!(map_partitions(vec![batch(1)], 1, |_| -> Result<Batch> { panic!("boom") }).is_err());
    }
}
