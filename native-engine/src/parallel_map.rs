//! Bounded worker execution for independent native partitions.
use std::{panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, OnceLock, atomic::{AtomicBool, Ordering}}};
use crate::{batch::Batch, Error, Result};

const MAX_OS_WORKERS: usize = 256;
// Amortize task queue synchronization for pipelines with tiny partitions.
const TASK_CLAIM_BATCH: usize = 16;

fn worker_count(partitions: usize, requested: usize) -> usize {
    partitions.min(requested).min(MAX_OS_WORKERS)
}

// Keep enough work available for other workers when the input is small.
// Large inputs still amortize the queue lock over up to 16 partitions.
fn claim_batch_size(partitions: usize, workers: usize) -> usize {
    TASK_CLAIM_BATCH.min(partitions.div_ceil(workers))
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
    let workers = worker_count(count, max_workers);
    let claim_size = claim_batch_size(count, workers);
    let tasks = Mutex::new(input.into_iter().enumerate());
    let failed = AtomicBool::new(false);
    // Each task has a unique index, so results can be published without a
    // contended global output mutex.
    let mut output: Vec<OnceLock<Result<Batch>>> =
        std::iter::repeat_with(OnceLock::new).take(count).collect();
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tasks = &tasks;
            let output = &output;
            let operator = &operator;
            let failed = &failed;
            scope.spawn(move || loop {
                if failed.load(Ordering::Acquire) { break; }
                // Claim several adjacent partitions under one mutex
                // acquisition, then run all operators outside the lock.
                // Use a bounded stack array instead of allocating a Vec
                // for every claim, especially costly for tiny partitions.
                let claimed: [Option<(usize, Batch)>; TASK_CLAIM_BATCH] = {
                    let mut queue = tasks.lock().unwrap_or_else(|e| e.into_inner());
                    std::array::from_fn(|index| if index < claim_size { queue.next() } else { None })
                };
                if claimed[0].is_none() { break; }
                for (index, batch) in claimed.into_iter().flatten() {
                    if failed.load(Ordering::Acquire) { break; }
                    let result = catch_unwind(AssertUnwindSafe(|| operator(batch)))
                        .unwrap_or_else(|_| Err(Error::Execution("native worker panicked".into())));
                    if result.is_err() { failed.store(true, Ordering::Release); }
                    let _ = output[index].set(result);
                }
            });
        }
    });
    // A cancelled task may precede the failed task in input order.
    // Return the actual operator error instead of a missing-result error.
    if let Some(index) = output.iter()
        .position(|slot| slot.get().is_some_and(Result::is_err)) {
        // Workers are joined. Move out the original error instead of
        // cloning its (possibly large) diagnostics under error fan-out.
        let Err(error) = output[index].take().expect("failed result is published") else {
            unreachable!("selected a failed result");
        };
        return Err(error);
    }
    output.into_iter()
        .map(|slot| slot.into_inner().unwrap_or_else(|| Err(Error::Execution("missing partition result".into()))))
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
    fn leaves_small_partition_sets_available_to_all_workers() {
        assert_eq!(claim_batch_size(4, 4), 1);
        assert_eq!(claim_batch_size(16, 4), 4);
        assert_eq!(claim_batch_size(30, 4), 8);
        assert_eq!(claim_batch_size(100, 4), 16);
        assert_eq!(claim_batch_size(1_000_000, 256), 16);
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
    map_partitions(input, max_workers, |batch| crate::filter::filter_owned(batch, &predicate))
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
mod contention_free_result_tests {
    use super::*;
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[test]
    fn many_partitions_publish_exactly_once_in_input_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let called = calls.clone();
        let input = (0..1024).map(|i| {
            Batch::from_rows(
                crate::schema::Schema::new(vec![crate::schema::Field::new(
                    "id", crate::schema::DataType::Integer, false
                )]).unwrap(),
                vec![vec![crate::scalar::Value::Integer(i)]],
            ).unwrap()
        }).collect();
        let result = map_partitions(input, 32, move |batch| {
            called.fetch_add(1, Ordering::Relaxed);
            Ok(batch)
        }).unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1024);
        assert_eq!(result.len(), 1024);
        for (i, batch) in result.iter().enumerate() {
            assert_eq!(batch.value(0, 0).unwrap(), &crate::scalar::Value::Integer(i as i64));
        }
    }
}

#[cfg(test)]
mod chunk_claim_parallel_map_tests {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn inputs(count: i64) -> Vec<Batch> {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        (0..count).map(|n| Batch::from_rows(schema.clone(),
            vec![vec![Value::Integer(n)]]).unwrap()).collect()
    }

    #[test]
    fn uneven_task_claims_process_every_partition_once_in_order() {
        for count in [1, 15, 16, 17, 63, 257, 1025] {
            let calls = AtomicUsize::new(0);
            let actual = map_partitions(inputs(count), 7, |batch| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(batch)
            }).unwrap();
            assert_eq!(calls.load(Ordering::Relaxed), count as usize);
            assert_eq!(actual.len(), count as usize);
            for (i, batch) in actual.iter().enumerate() {
                assert_eq!(batch.value(0, 0).unwrap(), &Value::Integer(i as i64));
            }
        }
    }

    #[test]
    fn failed_claim_reports_operator_error_not_a_missing_partition() {
        let result = map_partitions(inputs(257), 8, |batch| {
            if batch.value(0, 0)? == &Value::Integer(0) {
                return Err(Error::Execution("injected task failure".into()));
            }
            Ok(batch)
        });
        assert!(result.unwrap_err().to_string().contains("injected task failure"));
    }
}

#[cfg(test)]
mod fixed_claim_array_tests {
    use super::*;
    use crate::{schema::{DataType, Field, Schema}, scalar::Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn partial_final_claims_and_empty_queue_preserve_all_results() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        for count in [2usize, 16, 17, 31, 32, 33, 1027] {
            let input = (0..count).map(|n| Batch::from_rows(schema.clone(),
                vec![vec![Value::Integer(n as i64)]]).unwrap()).collect();
            let called = AtomicUsize::new(0);
            let results = map_partitions(input, 5, |batch| {
                called.fetch_add(1, Ordering::Relaxed);
                Ok(batch)
            }).unwrap();
            assert_eq!(called.load(Ordering::Relaxed), count);
            assert_eq!(results.len(), count);
            for (index, batch) in results.iter().enumerate() {
                assert_eq!(batch.value(0, 0).unwrap(), &Value::Integer(index as i64));
            }
        }
    }
}

#[cfg(test)]
mod owned_parallel_error_regressions {
    use super::*;
    use crate::{scalar::Value, schema::{DataType, Field, Schema}};

    #[test]
    fn preserves_actual_error_after_skipping_unstarted_partitions() {
        let schema = Schema::new(vec![Field::new("id", DataType::Integer, false)]).unwrap();
        let input = (0..64).map(|n| Batch::from_rows(
            schema.clone(), vec![vec![Value::Integer(n)]],
        ).unwrap()).collect();
        let expected = "operator failed: ".to_string() + &"x".repeat(8192);
        let result = map_partitions(input, 8, |batch| {
            if batch.value(0, 0)? == &Value::Integer(0) {
                return Err(Error::Execution(expected.clone()));
            }
            Ok(batch)
        });
        let error = result.unwrap_err().to_string();
        assert!(error.contains("operator failed: "));
        assert!(error.contains(&"x".repeat(8192)));
    }
}
