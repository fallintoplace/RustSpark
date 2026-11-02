use rustspark_native::{
    local_workers::execute_local_graph,
    stage_graph::{Stage, StageGraph},
    Error,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn parallel_partition_workers_respect_stage_dependencies_and_order() {
    let graph = StageGraph::new(vec![
        Stage { id: 20, dependencies: vec![10], partitions: 4 },
        Stage { id: 10, dependencies: vec![], partitions: 64 },
    ]).unwrap();
    let completed = AtomicUsize::new(0);
    let output = execute_local_graph(&graph, 8, 128, |stage, partition| {
        if stage == 10 {
            completed.fetch_add(1, Ordering::SeqCst);
        } else {
            assert_eq!(completed.load(Ordering::SeqCst), 64);
        }
        Ok(stage + partition as u64)
    }).unwrap();
    assert_eq!(output.len(), 68);
    assert_eq!(output[0], (10, 0, 10));
    assert_eq!(output[63], (10, 63, 73));
    assert_eq!(output[64], (20, 0, 20));
    assert_eq!(completed.load(Ordering::SeqCst), 64);
}

#[test]
fn rejects_invalid_budgets_and_propagates_task_errors() {
    let graph = StageGraph::new(vec![
        Stage { id: 1, dependencies: vec![], partitions: 8 },
    ]).unwrap();
    assert!(execute_local_graph(&graph, 0, 10, |_, _| Ok(1)).is_err());
    assert!(execute_local_graph(&graph, 2, 7, |_, _| Ok(1)).is_err());
    let failure = execute_local_graph(&graph, 4, 8, |_, partition| {
        if partition == 0 { Err(Error::Execution("worker failed".into())) }
        else { Ok(partition) }
    }).unwrap_err();
    assert!(failure.to_string().contains("worker failed"));
}


#[test]
fn independent_stages_run_concurrently_without_releasing_dependents_early() {
    use rustspark_native::local_workers::execute_local_graph_concurrent;
    use std::{sync::Arc, time::{Duration, Instant}};
    let graph = StageGraph::new(vec![
        Stage { id: 30, dependencies: vec![10, 20], partitions: 1 },
        Stage { id: 20, dependencies: vec![], partitions: 1 },
        Stage { id: 10, dependencies: vec![], partitions: 1 },
    ]).unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let results = execute_local_graph_concurrent(&graph, 2, 3, |id, _| {
        if id == 30 {
            assert_eq!(completed.load(Ordering::SeqCst), 2);
            return Ok(id);
        }
        started.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();
        while started.load(Ordering::SeqCst) < 2 {
            if start.elapsed() > Duration::from_secs(2) {
                return Err(Error::Execution("ready stages were serialized".into()));
            }
            std::thread::yield_now();
        }
        completed.fetch_add(1, Ordering::SeqCst);
        Ok(id)
    }).unwrap();
    assert_eq!(results, vec![(10, 0, 10), (20, 0, 20), (30, 0, 30)]);
}

#[test]
fn concurrent_stage_execution_propagates_panics_and_enforces_global_budget() {
    use rustspark_native::local_workers::execute_local_graph_concurrent;
    let graph = StageGraph::new(vec![
        Stage { id: 2, dependencies: vec![], partitions: 3 },
        Stage { id: 1, dependencies: vec![], partitions: 2 },
    ]).unwrap();
    assert!(execute_local_graph_concurrent(&graph, 2, 4, |_, _| Ok(1)).is_err());
    assert!(execute_local_graph_concurrent(&graph, 0, 5, |_, _| Ok(1)).is_err());
    let failed = execute_local_graph_concurrent(&graph, 3, 5, |id, partition| {
        if id == 1 && partition == 1 { panic!("worker failed"); }
        Ok(partition)
    }).unwrap_err();
    assert!(failed.to_string().contains("panicked"));
    let rows = execute_local_graph_concurrent(&graph, 1, 5,
        |id, partition| Ok((id, partition))).unwrap();
    assert_eq!(rows, vec![
        (1, 0, (1, 0)), (1, 1, (1, 1)),
        (2, 0, (2, 0)), (2, 1, (2, 1)), (2, 2, (2, 2)),
    ]);
}
