use rustspark_native::exchange_commit::CommitLedger;

#[test]
fn retries_fence_old_chunks_and_commit_each_partition_once() {
    let mut ledger = CommitLedger::new(3, 4).unwrap();
    assert!(ledger.record_chunk(1, 2, 1, 0, 10).unwrap());
    assert!(!ledger.record_chunk(1, 2, 1, 0, 10).unwrap());
    assert!(ledger.record_chunk(1, 2, 1, 0, 11).is_err());
    assert!(ledger.record_chunk(1, 2, 1, 2, 12).is_err());
    assert!(ledger.record_chunk(1, 2, 2, 0, 20).unwrap());
    assert!(!ledger.record_chunk(1, 2, 1, 1, 13).unwrap());
    assert!(ledger.record_chunk(1, 2, 2, 1, 21).unwrap());
    assert!(ledger.commit(1, 2, 2, 3).is_err());
    assert!(ledger.commit(1, 2, 2, 2).unwrap());
    assert!(ledger.is_committed(1, 2));
    assert!(!ledger.commit(1, 2, 2, 2).unwrap());
    assert!(!ledger.record_chunk(1, 2, 3, 0, 30).unwrap());
    assert!(!ledger.record_chunk(1, 2, 2, 0, 20).unwrap());
    assert!(ledger.record_chunk(1, 2, 2, 1, 22).is_err());
}

#[test]
fn enforces_partition_and_chunk_budgets() {
    let mut ledger = CommitLedger::new(1, 1).unwrap();
    assert!(ledger.record_chunk(2, 4, 0, 0, 99).unwrap());
    assert!(ledger.record_chunk(2, 4, 0, 1, 99).is_err());
    assert!(ledger.record_chunk(2, 5, 0, 0, 99).is_err());
    assert_eq!(ledger.tracked_partitions(), 1);
}


#[test]
fn atomic_chunk_windows_rollback_on_late_conflicts_and_capacity() {
    use rustspark_native::exchange_commit::ChunkRecord;
    let chunk = |stage, partition, attempt, sequence, checksum| ChunkRecord {
        stage, partition, attempt, sequence, checksum
    };
    let mut ledger = CommitLedger::new(2, 3).unwrap();
    let fail = [chunk(1, 1, 0, 0, 11), chunk(1, 1, 0, 2, 13)];
    assert!(ledger.record_chunks_atomic(&fail).is_err());
    assert_eq!(ledger.tracked_partitions(), 0);
    assert_eq!(ledger.record_chunks_atomic(&[
        chunk(1, 1, 0, 0, 11), chunk(1, 1, 0, 1, 12),
        chunk(1, 1, 0, 1, 12),
    ]).unwrap(), vec![true, true, false]);
    assert_eq!(ledger.tracked_partitions(), 1);
    assert!(ledger.record_chunks_atomic(&[
        chunk(1, 1, 1, 0, 21), chunk(1, 1, 1, 1, 22),
        chunk(1, 1, 1, 1, 23),
    ]).is_err());
    // Failed speculative takeover did not replace attempt zero.
    assert!(!ledger.record_chunk(1, 1, 0, 0, 11).unwrap());
    assert!(ledger.commit(1, 1, 0, 2).unwrap());
    assert!(ledger.record_chunks_atomic(&[
        chunk(2, 2, 0, 0, 30), chunk(3, 3, 0, 0, 31),
    ]).is_err());
    assert_eq!(ledger.tracked_partitions(), 1);
    assert_eq!(ledger.record_chunks_atomic(&[chunk(2, 2, 0, 0, 30)]).unwrap(), vec![true]);
    assert_eq!(ledger.tracked_partitions(), 2);
}

#[test]
fn atomic_chunk_windows_keep_stale_attempts_and_commits_fenced() {
    use rustspark_native::exchange_commit::ChunkRecord;
    let mut ledger = CommitLedger::new(2, 2).unwrap();
    ledger.record_chunk(4, 8, 5, 0, 42).unwrap();
    ledger.commit(4, 8, 5, 1).unwrap();
    let result = ledger.record_chunks_atomic(&[
        ChunkRecord { stage: 4, partition: 8, attempt: 6, sequence: 0, checksum: 99 },
        ChunkRecord { stage: 4, partition: 8, attempt: 5, sequence: 0, checksum: 42 },
    ]).unwrap();
    assert_eq!(result, vec![false, false]);
    assert!(ledger.is_committed(4, 8));
    assert_eq!(ledger.record_chunks_atomic(&[]).unwrap(), Vec::<bool>::new());
}
