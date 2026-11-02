use rustspark_native::task_scheduler::{TaskCounts, TaskScheduler, TaskStatus};
use std::time::{Duration, Instant};

#[test]
fn completion_batch_rejects_one_stale_attempt_without_partial_commits() {
    let now = Instant::now();
    let mut scheduler = TaskScheduler::new([10, 20, 30], 3).unwrap();
    let leases = scheduler.lease_many(5, now, Duration::from_secs(10), 3).unwrap();
    let mut stale = leases[2].clone();
    stale.attempt += 1;
    let state = scheduler.counts();
    assert!(!scheduler.finish_many_atomic(&[
        (leases[0].clone(), true),
        (leases[1].clone(), true),
        (stale, true),
    ], now).unwrap());
    assert_eq!(scheduler.counts(), state);
    for lease in &leases {
        assert!(matches!(scheduler.status(lease.task), Some(TaskStatus::Leased { .. })));
    }
    assert!(scheduler.finish_many_atomic(&[
        (leases[0].clone(), true),
        (leases[1].clone(), false),
        (leases[2].clone(), true),
    ], now).unwrap());
    assert_eq!(scheduler.counts(), TaskCounts {
        queued: 1, leased: 0, done: 2, failed: 0,
    });
    assert_eq!(scheduler.queued_tasks(3), vec![20]);
}

#[test]
fn duplicate_attempts_and_expired_leases_never_partially_commit() {
    let now = Instant::now();
    let mut scheduler = TaskScheduler::new([1, 2], 2).unwrap();
    let leases = scheduler.lease_many(7, now, Duration::from_secs(2), 2).unwrap();
    let state = scheduler.counts();
    assert!(scheduler.finish_many_atomic(&[
        (leases[0].clone(), true), (leases[0].clone(), false),
    ], now).is_err());
    assert_eq!(scheduler.counts(), state);
    assert!(!scheduler.finish_many_atomic(&[
        (leases[0].clone(), true), (leases[1].clone(), true),
    ], now + Duration::from_secs(2)).unwrap());
    assert_eq!(scheduler.counts(), state);
    assert!(scheduler.finish_many_atomic(&[], now).unwrap());
    assert!(scheduler.finish_many_atomic(&[
        (leases[1].clone(), true), (leases[0].clone(), true),
    ], now + Duration::from_secs(1)).unwrap());
    assert_eq!(scheduler.counts().done, 2);
}

#[test]
fn atomic_completion_respects_retry_exhaustion_and_cross_worker_leases() {
    let now = Instant::now();
    let mut scheduler = TaskScheduler::new([1, 2], 1).unwrap();
    let one = scheduler.lease(10, now, Duration::from_secs(10)).unwrap().unwrap();
    let two = scheduler.lease(11, now, Duration::from_secs(10)).unwrap().unwrap();
    let mut foreign = two.clone();
    foreign.worker = 10;
    assert!(!scheduler.finish_many_atomic(&[
        (one.clone(), true), (foreign, true),
    ], now).unwrap());
    assert!(scheduler.finish_many_atomic(&[
        (one, true), (two, false),
    ], now).unwrap());
    assert_eq!(scheduler.counts(), TaskCounts {
        queued: 0, leased: 0, done: 1, failed: 1,
    });
}
