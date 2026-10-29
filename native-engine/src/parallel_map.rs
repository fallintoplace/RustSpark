//! Bounded worker execution for independent native partitions.
use std::{collections::VecDeque, panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, atomic::{AtomicBool, Ordering}}};
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
    let failed = AtomicBool::new(false);
    let output: Mutex<Vec<Option<Result<Batch>>>> =
        Mutex::new(std::iter::repeat_with(|| None).take(count).collect());
    std::thread::scope(|scope| {
        for _ in 0..max_workers.min(count) {
            let tasks = &tasks;
            let output = &output;
            let operator = &operator;
            let failed = &failed;
            scope.spawn(move || loop {
                if failed.load(Ordering::Acquire) { break; }
                let next = tasks.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
                let Some((index, batch)) = next else { break; };
                let result = catch_unwind(AssertUnwindSafe(|| operator(batch)))
                    .unwrap_or_else(|_| Err(Error::Execution("native worker panicked".into())));
                if result.is_err() { failed.store(true, Ordering::Release); }
                output.lock().unwrap_or_else(|e| e.into_inner())[index] = Some(result);
            });
        }
    });
    let output = output.into_inner().unwrap_or_else(|e| e.into_inner());
    // A cancelled task may precede the failed task in input order.
    // Return the actual operator error instead of a missing-result error.
    if let Some(error) = output.iter().filter_map(Option::as_ref)
        .find_map(|result| result.as_ref().err()) {
        return Err(error.clone());
    }
    output.into_iter()
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

 
/// Apply an independent SQL predicate to every partition in parallel.
pub fn filter_partitions(
    input: Vec<Batch>,
    max_workers: usize,
    predicate: crate::expr::Expr,
) -> Result<Vec<Batch>> {
    map_partitions(input, max_workers, |batch| crate::filter::filter(&batch, &predicate))
}

#[cfg(test)]
mod parallel_filter_feature_tests {
    use super::*;
    use crate::{expr::{Expr, BinaryOp}, scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn keeps_partition_order_and_sql_true_rows_only() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, true)]).unwrap();
        let parts: Vec<_> = (0..12).map(|n| {
            Batch::from_rows(schema.clone(), vec![
                vec![Value::Integer(n)], vec![Value::Null],
            ]).unwrap()
        }).collect();
        let predicate = Expr::binary(Expr::Column("n".into()),
            BinaryOp::GreaterEqual, Expr::Literal(Value::Integer(8)));
        let filtered = filter_partitions(parts, 3, predicate).unwrap();
        assert_eq!(filtered.iter().map(Batch::num_rows).sum::<usize>(), 4);
        assert_eq!(filtered[8].row(0).unwrap(), vec![Value::Integer(8)]);
        assert_eq!(filtered[0].num_rows(), 0);
    }
}


#[cfg(test)]
mod parallel_fail_fast_engine_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn serial_worker_stops_after_first_failure() {
        let batch = Batch::from_rows(
            Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap(),
            vec![vec![Value::Integer(1)]],
        ).unwrap();
        let invoked = AtomicUsize::new(0);
        let error = map_partitions(vec![batch; 20], 1, |_| {
            invoked.fetch_add(1, Ordering::SeqCst);
            Err(Error::Execution("worker failed".into()))
        }).unwrap_err();
        assert!(error.to_string().contains("worker failed"));
        assert_eq!(invoked.load(Ordering::SeqCst), 1);
    }
}
