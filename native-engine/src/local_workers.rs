//! Dependency-respecting bounded worker pool for the native stage DAG.
//! This is a local parallel execution primitive, not a network scheduler.
use std::{
    collections::HashMap,
    sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, Mutex},
};

use crate::{stage_graph::{Stage, StageGraph}, Error, Result};

/// Execute partition tasks concurrently within each stage and complete all
/// dependencies before starting a dependent stage. Results are emitted in
/// stable (topological stage, partition) order, regardless of worker timing.
///
/// The result size per partition remains the caller's responsibility; this
/// function bounds the task count, not the byte size of arbitrary T values.
pub fn execute_local_graph<T, F>(
    graph: &StageGraph, max_workers: usize, max_tasks: usize, run: F,
) -> Result<Vec<(u64, usize, T)>>
where
    T: Send,
    F: Fn(u64, usize) -> Result<T> + Sync,
{
    if max_workers == 0 || max_workers > 64 || max_tasks == 0 {
        return Err(Error::Execution("invalid local stage worker budgets".into()));
    }
    let mut total = 0usize;
    let mut by_id: HashMap<u64, &Stage> = HashMap::new();
    by_id.try_reserve(graph.stages().len())
        .map_err(|_| Error::Execution("local stage index allocation failed".into()))?;
    for stage in graph.stages() {
        total = total.checked_add(stage.partitions)
            .filter(|&rows| rows <= max_tasks)
            .ok_or_else(|| Error::Execution("local stage task budget exceeded".into()))?;
        by_id.insert(stage.id, stage);
    }
    let mut all = Vec::new();
    all.try_reserve_exact(total)
        .map_err(|_| Error::Execution("local stage result allocation failed".into()))?;
    for &stage_id in graph.topological_order() {
        let stage = by_id.get(&stage_id)
            .ok_or_else(|| Error::Execution("unknown native stage".into()))?;
        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let error: Mutex<Option<Error>> = Mutex::new(None);
        let mut slots = Vec::new();
        slots.try_reserve_exact(stage.partitions)
            .map_err(|_| Error::Execution("local stage slot allocation failed".into()))?;
        slots.resize_with(stage.partitions, || None::<T>);
        let results = Mutex::new(slots);
        let spawn_result: Result<()> = std::thread::scope(|scope| {
            for _ in 0..max_workers.min(stage.partitions) {
                let next = &next;
                let failed = &failed;
                let error = &error;
                let results = &results;
                let task = &run;
                // Worker tasks may come from an embedding application.
                // Report their panics as query failures instead of unwinding the
                // entire scoped pool and losing the original stage context.
                let spawn = std::thread::Builder::new().spawn_scoped(scope, move || {
                    loop {
                        if failed.load(Ordering::Acquire) { break; }
                        let partition = next.fetch_add(1, Ordering::Relaxed);
                        if partition >= stage.partitions { break; }
                        let outcome = std::panic::catch_unwind(
                            std::panic::AssertUnwindSafe(|| task(stage_id, partition))
                        );
                        match outcome {
                            Ok(Ok(value)) => {
                                results.lock().expect("stage results mutex poisoned")[partition]
                                    = Some(value);
                            }
                            Ok(Err(err)) => {
                                if !failed.swap(true, Ordering::AcqRel) {
                                    *error.lock().expect("stage error mutex poisoned") = Some(err);
                                }
                                break;
                            }
                            Err(_) => {
                                if !failed.swap(true, Ordering::AcqRel) {
                                    *error.lock().expect("stage error mutex poisoned") = Some(
                                        Error::Execution("native local stage worker panicked".into())
                                    );
                                }
                                break;
                            }
                        }
                    }
                });
                if spawn.is_err() {
                    failed.store(true, Ordering::Release);
                    return Err(Error::Execution("failed to spawn local stage worker".into()));
                }
            }
            Ok(())
        });
        spawn_result?;
        if let Some(err) = error.into_inner()
            .map_err(|_| Error::Execution("local stage error lock poisoned".into()))? {
            return Err(err);
        }
        let slots = results.into_inner()
            .map_err(|_| Error::Execution("local stage result lock poisoned".into()))?;
        for (partition, value) in slots.into_iter().enumerate() {
            let value = value.ok_or_else(|| Error::Execution(
                "local stage worker did not return a partition".into()))?;
            all.push((stage_id, partition, value));
        }
    }
    Ok(all)
}


/// Execute independent ready stages in parallel, while preserving every DAG
/// dependency and returning results in deterministic topological order.
///
/// This is a wave scheduler: a stage becomes eligible after its dependencies
/// have *all* completed. The shared worker limit applies to the entire ready
/// wave, rather than being multiplied by the number of ready stages.
/// Like execute_local_graph, this bounds task count, not result byte size.
pub fn execute_local_graph_concurrent<T, F>(
    graph: &StageGraph, max_workers: usize, max_tasks: usize, run: F,
) -> Result<Vec<(u64, usize, T)>>
where
    T: Send,
    F: Fn(u64, usize) -> Result<T> + Sync,
{
    use std::collections::HashSet;

    if max_workers == 0 || max_workers > 64 || max_tasks == 0 {
        return Err(Error::Execution("invalid local stage worker budgets".into()));
    }
    let mut total = 0usize;
    let mut indices = HashMap::new();
    indices.try_reserve(graph.stages().len())
        .map_err(|_| Error::Execution("local stage index allocation failed".into()))?;
    let mut slots = Vec::new();
    slots.try_reserve_exact(graph.stages().len())
        .map_err(|_| Error::Execution("local stage slot allocation failed".into()))?;
    for &stage_id in graph.topological_order() {
        let stage = graph.stages().iter().find(|stage| stage.id == stage_id)
            .ok_or_else(|| Error::Execution("unknown native stage".into()))?;
        total = total.checked_add(stage.partitions)
            .filter(|&count| count <= max_tasks)
            .ok_or_else(|| Error::Execution("local stage task budget exceeded".into()))?;
        let mut partition_slots = Vec::new();
        partition_slots.try_reserve_exact(stage.partitions)
            .map_err(|_| Error::Execution("local stage slot allocation failed".into()))?;
        partition_slots.resize_with(stage.partitions, || None::<T>);
        indices.insert(stage_id, (slots.len(), stage.partitions));
        slots.push(partition_slots);
    }

    let slots = Mutex::new(slots);
    let mut completed = HashSet::new();
    completed.try_reserve(graph.stages().len())
        .map_err(|_| Error::Execution("local completed stage allocation failed".into()))?;

    while completed.len() < graph.stages().len() {
        let ready = graph.ready(&completed);
        if ready.is_empty() {
            return Err(Error::Execution("native stage graph made no progress".into()));
        }
        let mut tasks = Vec::new();
        let wave_size = ready.iter().map(|id| indices[id].1).sum::<usize>();
        tasks.try_reserve_exact(wave_size)
            .map_err(|_| Error::Execution("local ready task allocation failed".into()))?;
        for &stage_id in &ready {
            let (stage_index, partitions) = indices[&stage_id];
            for partition in 0..partitions {
                tasks.push((stage_id, stage_index, partition));
            }
        }

        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let error = Mutex::new(None::<Error>);
        let spawn_result: Result<()> = std::thread::scope(|scope| {
            for _ in 0..max_workers.min(tasks.len()) {
                let tasks = &tasks;
                let next = &next;
                let failed = &failed;
                let error = &error;
                let slots = &slots;
                let run = &run;
                let spawned = std::thread::Builder::new().spawn_scoped(scope, move || {
                    loop {
                        if failed.load(Ordering::Acquire) { break; }
                        let next_task = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(stage_id, stage_index, partition)) = tasks.get(next_task)
                            else { break; };
                        let outcome = std::panic::catch_unwind(
                            std::panic::AssertUnwindSafe(|| run(stage_id, partition))
                        );
                        match outcome {
                            Ok(Ok(value)) => {
                                slots.lock().expect("stage results mutex poisoned")
                                    [stage_index][partition] = Some(value);
                            }
                            Ok(Err(err)) => {
                                if !failed.swap(true, Ordering::AcqRel) {
                                    *error.lock().expect("stage error mutex poisoned") = Some(err);
                                }
                                break;
                            }
                            Err(_) => {
                                if !failed.swap(true, Ordering::AcqRel) {
                                    *error.lock().expect("stage error mutex poisoned") = Some(
                                        Error::Execution("native local stage worker panicked".into())
                                    );
                                }
                                break;
                            }
                        }
                    }
                });
                if spawned.is_err() {
                    failed.store(true, Ordering::Release);
                    return Err(Error::Execution("failed to spawn local stage worker".into()));
                }
            }
            Ok(())
        });
        spawn_result?;
        if let Some(err) = error.into_inner()
            .map_err(|_| Error::Execution("local stage error lock poisoned".into()))? {
            return Err(err);
        }
        completed.extend(ready);
    }

    let mut output = Vec::new();
    output.try_reserve_exact(total)
        .map_err(|_| Error::Execution("local stage result allocation failed".into()))?;
    let slots = slots.into_inner()
        .map_err(|_| Error::Execution("local stage result lock poisoned".into()))?;
    for (stage_id, rows) in graph.topological_order().iter().copied().zip(slots) {
        for (partition, result) in rows.into_iter().enumerate() {
            output.push((stage_id, partition, result.ok_or_else(|| Error::Execution(
                "local stage worker did not return a partition".into()
            ))?));
        }
    }
    Ok(output)
}

#[cfg(test)]
mod local_worker_failure_isolation_tests {
    use super::*;

    #[test]
    fn user_task_panic_becomes_a_query_error_without_unwinding_caller() {
        let graph = StageGraph::new(vec![Stage {
            id: 3, dependencies: vec![], partitions: 4,
        }]).unwrap();
        let result = execute_local_graph(&graph, 4, 4, |_stage, part| -> Result<usize> {
            if part == 2 { panic!("task crashed"); }
            Ok(part)
        });
        assert!(result.unwrap_err().to_string().contains("panicked"));
    }

    #[test]
    fn multiple_stages_keep_deterministic_partition_order() {
        let graph = StageGraph::new(vec![
            Stage { id: 5, dependencies: vec![1], partitions: 3 },
            Stage { id: 1, dependencies: vec![], partitions: 2 },
        ]).unwrap();
        let rows = execute_local_graph(&graph, 3, 5, |stage, part| {
            Ok(format!("{stage}:{part}"))
        }).unwrap();
        assert_eq!(rows, vec![
            (1, 0, "1:0".to_owned()), (1, 1, "1:1".to_owned()),
            (5, 0, "5:0".to_owned()), (5, 1, "5:1".to_owned()),
            (5, 2, "5:2".to_owned()),
        ]);
    }
}
