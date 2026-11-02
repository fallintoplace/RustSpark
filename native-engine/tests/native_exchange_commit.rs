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
