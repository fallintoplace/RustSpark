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
        std::thread::scope(|scope| {
            for _ in 0..max_workers.min(stage.partitions) {
                let next = &next;
                let failed = &failed;
                let error = &error;
                let results = &results;
                let task = &run;
                scope.spawn(move || {
                    loop {
                        if failed.load(Ordering::Acquire) { break; }
                        let partition = next.fetch_add(1, Ordering::Relaxed);
                        if partition >= stage.partitions { break; }
                        match task(stage_id, partition) {
                            Ok(value) => {
                                results.lock().expect("stage results mutex poisoned")[partition]
                                    = Some(value);
                            }
                            Err(err) => {
                                if !failed.swap(true, Ordering::AcqRel) {
                                    *error.lock().expect("stage error mutex poisoned") = Some(err);
                                }
                                break;
                            }
                        }
                    }
                });
            }
        });
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
