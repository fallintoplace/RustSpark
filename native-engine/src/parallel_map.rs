//! Bounded worker execution for independent native partitions.
use std::{panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, atomic::{AtomicBool, Ordering}}};
use crate::{batch::Batch, Error, Result};

const MAX_OS_WORKERS: usize = 256;

fn worker_count(partitions: usize, requested: usize) -> usize {
    partitions.min(requested).min(MAX_OS_WORKERS)
}

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
    if max_workers == 1 || count == 1 {
        let mut results = Vec::with_capacity(count);
        for batch in input {
            results.push(catch_unwind(AssertUnwindSafe(|| operator(batch)))
                .unwrap_or_else(|_| Err(Error::Execution("native worker panicked".into())))?);
        }
        return Ok(results);
    }
    let tasks = Mutex::new(input.into_iter().enumerate());
    let failed = AtomicBool::new(false);
    let output: Mutex<Vec<Option<Result<Batch>>>> =
        Mutex::new(std::iter::repeat_with(|| None).take(count).collect());
    std::thread::scope(|scope| {
        for _ in 0..worker_count(count, max_workers) {
            let tasks = &tasks;
            let output = &output;
            let operator = &operator;
            let failed = &failed;
            scope.spawn(move || loop {
                if failed.load(Ordering::Acquire) { break; }
                let next = tasks.lock().unwrap_or_else(|e| e.into_inner()).next();
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
    fn bounds_worker_spawns_for_untrusted_concurrency_settings() {
        assert_eq!(worker_count(1_000_000, usize::MAX), MAX_OS_WORKERS);
        assert_eq!(worker_count(4, usize::MAX), 4);
        assert_eq!(worker_count(100, 3), 3);
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

 
/// Run independent native partitions with cooperative query deadlines and cancellation.
pub fn map_partitions_guarded<F>(
    input: Vec<Batch>, max_workers: usize,
    guard: &crate::cancel::QueryGuard, operator: F,
) -> Result<Vec<Batch>>
where
    F: Fn(Batch) -> Result<Batch> + Sync,
{
    guard.check()?;
    let output = map_partitions(input, max_workers, |batch| {
        guard.check()?;
        let output = operator(batch)?;
        guard.check()?;
        Ok(output)
    })?;
    guard.check()?;
    Ok(output)
}

/// Apply an independent SQL predicate to every partition in parallel.
pub fn filter_partitions(
    input: Vec<Batch>,
    max_workers: usize,
    predicate: crate::expr::Expr,
) -> Result<Vec<Batch>> {
    map_partitions(input, max_workers, |batch| crate::filter::filter(&batch, &predicate))
}

/// Bound concurrent filtering and propagate query cancellation into each
/// worker's row selection, not only its partition boundaries.
pub fn filter_partitions_guarded(
    input: Vec<Batch>,
    max_workers: usize,
    predicate: crate::expr::Expr,
    guard: &crate::cancel::QueryGuard,
) -> Result<Vec<Batch>> {
    map_partitions_guarded(input, max_workers, guard, |batch| {
        crate::filter::filter_checked(&batch, &predicate, || guard.check())
    })
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

#[cfg(test)]
mod serial_worker_fast_path_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn serial_map_retains_input_order_and_stops_on_failure() {
        let schema = Schema::new(vec![Field::new("n", DataType::Integer, false)]).unwrap();
        let batches = (0..4).map(|n| Batch::from_rows(schema.clone(), vec![vec![Value::Integer(n)]]).unwrap()).collect::<Vec<_>>();
        let calls = AtomicUsize::new(0);
        let result = map_partitions(batches, 1, |batch| {
            let call = calls.fetch_add(1, Ordering::Relaxed);
            if call == 2 { return Err(Error::Execution("stop".into())); }
            Ok(batch)
        });
        assert!(result.unwrap_err().to_string().contains("stop"));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}

#[cfg(test)]
mod native_cancellable_parallel_worker_tests {
    use super::*;
    use crate::{cancel::{Cancellation, QueryGuard}, scalar::Value,
        schema::{DataType, Field, Schema}};

    #[test]
    fn cancellation_rejects_work_before_dispatch_and_after_execution() {
        let schema = Schema::new(vec![Field::new("value", DataType::Integer, false)]).unwrap();
        let batch = Batch::from_rows(schema, vec![vec![Value::Integer(3)]]).unwrap();
        let cancellation = Cancellation::new();
        let guard = QueryGuard::new(cancellation.clone());
        assert_eq!(map_partitions_guarded(vec![batch.clone()], 1, &guard, Ok).unwrap(),
            vec![batch.clone()]);
        cancellation.cancel();
        assert!(map_partitions_guarded(vec![batch.clone()], 1, &guard, Ok).is_err());
        let token = Cancellation::new();
        let running = QueryGuard::new(token.clone());
        let result = map_partitions_guarded(vec![batch], 1, &running, |batch| {
            token.cancel();
            Ok(batch)
        });
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod parallel_filter_guard_integration_tests {
    use super::*;
    use crate::{
        cancel::{Cancellation, QueryGuard},
        expr::{BinaryOp, Expr},
        scalar::Value,
        schema::{DataType, Field, Schema},
    };

    #[test]
    fn guarded_parallel_filter_preserves_order_and_honors_cancel() {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Integer, false),
        ]).unwrap();
        let batches = (0..4).map(|partition| {
            Batch::from_rows(schema.clone(), (0..2048)
                .map(|n| vec![Value::Integer(partition * 2048 + n)])
                .collect()).unwrap()
        }).collect::<Vec<_>>();
        let predicate = Expr::binary(
            Expr::Column("value".into()), BinaryOp::GreaterEqual,
            Expr::Literal(Value::Integer(4096)),
        );
        let token = Cancellation::new();
        let guard = QueryGuard::new(token.clone());
        let output = filter_partitions_guarded(
            batches.clone(), 4, predicate.clone(), &guard,
        ).unwrap();
        assert_eq!(output.iter().map(Batch::num_rows).collect::<Vec<_>>(),
            vec![0, 0, 2048, 2048]);
        assert_eq!(output[2].value(0, 0).unwrap(), &Value::Integer(4096));
        token.cancel();
        assert!(filter_partitions_guarded(batches, 4, predicate, &guard)
            .unwrap_err().to_string().contains("cancelled"));
    }
}
