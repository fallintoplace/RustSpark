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
