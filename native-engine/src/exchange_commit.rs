//! In-memory attempt fencing for shuffle chunks and partition publication.
//! Callers must durably store each accepted chunk before acknowledging it.
//! This is not a crash-recovery protocol or an on-wire frame version.
use std::collections::{hash_map::Entry, HashMap};

use crate::{Error, Result};

#[derive(Clone, Debug)]
struct PartitionState {
    attempt: u32,
    checksums: Vec<u32>,
    committed: bool,
}

/// A received chunk in a multi-frame shuffle window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRecord {
    pub stage: u64,
    pub partition: u64,
    pub attempt: u32,
    pub sequence: usize,
    pub checksum: u32,
}

/// Track ordered shuffle chunks and enforce one logical partition commit.
/// A newer attempt invalidates all uncommitted chunks from older attempts.
pub struct CommitLedger {
    states: HashMap<(u64, u64), PartitionState>,
    max_partitions: usize,
    max_chunks_per_partition: usize,
}

impl CommitLedger {
    pub fn new(max_partitions: usize, max_chunks_per_partition: usize) -> Result<Self> {
        if max_partitions == 0 || max_chunks_per_partition == 0 {
            return Err(Error::Execution("shuffle commit budgets must be positive".into()));
        }
        Ok(Self {
            states: HashMap::new(), max_partitions, max_chunks_per_partition,
        })
    }

    /// Return true only when a new chunk should be stored. Exact replay is
    /// idempotent. Replayed sequence IDs with different checksums fail closed.
    pub fn record_chunk(
        &mut self, stage: u64, partition: u64,
        attempt: u32, sequence: usize, checksum: u32,
    ) -> Result<bool> {
        let key = (stage, partition);
        let at_capacity = self.states.len() >= self.max_partitions;
        let state = match self.states.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if at_capacity {
                    return Err(Error::Execution("shuffle partition tracking budget exceeded".into()));
                }
                if sequence != 0 {
                    return Err(Error::Execution("first shuffle chunk must use sequence zero".into()));
                }
                // Invalid first frames must not occupy a partition slot.
                // Reserve before publishing any tracking state.
                let mut checksums = Vec::new();
                checksums.try_reserve_exact(1)
                    .map_err(|_| Error::Execution("shuffle chunk tracking allocation failed".into()))?;
                checksums.push(checksum);
                entry.insert(PartitionState {
                    attempt, checksums, committed: false,
                });
                return Ok(true);
            }
        };
        if attempt < state.attempt { return Ok(false); }
        if state.committed {
            // No speculative attempt may replace a published partition.
            if attempt > state.attempt { return Ok(false); }
            return match state.checksums.get(sequence) {
                Some(existing) if *existing == checksum => Ok(false),
                _ => Err(Error::Execution("committed shuffle chunk mismatch".into())),
            };
        }
        if attempt > state.attempt {
            if sequence != 0 {
                return Err(Error::Execution("new shuffle attempt must start at sequence zero".into()));
            }
            // Check allocation before abandoning the preceding attempt.
            let mut next = Vec::new();
            next.try_reserve_exact(1)
                .map_err(|_| Error::Execution("shuffle chunk tracking allocation failed".into()))?;
            state.checksums = next;
            state.attempt = attempt;
        }
        if sequence < state.checksums.len() {
            return if state.checksums[sequence] == checksum {
                Ok(false)
            } else {
                Err(Error::Execution("conflicting shuffle frame replay".into()))
            };
        }
        if sequence != state.checksums.len() {
            return Err(Error::Execution("out-of-order shuffle chunk sequence".into()));
        }
        if state.checksums.len() >= self.max_chunks_per_partition {
            return Err(Error::Execution("shuffle chunk count budget exceeded".into()));
        }
        state.checksums.try_reserve(1)
            .map_err(|_| Error::Execution("shuffle chunk tracking allocation failed".into()))?;
        state.checksums.push(checksum);
        Ok(true)
    }

    /// Admit a network window as one transaction. Invalid later frames must
    /// not leave earlier chunks or newer attempts visible in the live ledger.
    /// Only touched partition state is copied; unrelated partitions are never
    /// duplicated, so large long-lived ledgers remain cheap to update.
    ///
    /// The returned bool for each frame reports whether that chunk is new.
    pub fn record_chunks_atomic(&mut self, chunks: &[ChunkRecord]) -> Result<Vec<bool>> {
        let mut accepted = Vec::new();
        accepted.try_reserve_exact(chunks.len())
            .map_err(|_| Error::Execution("shuffle batch result allocation failed".into()))?;

        // Stage only touched partitions. The staging ledger reuses the exact
        // attempt fencing and contiguous-sequence checks from record_chunk.
        let mut staged = Self::new(self.max_partitions, self.max_chunks_per_partition)?;
        for chunk in chunks {
            let key = (chunk.stage, chunk.partition);
            if !staged.states.contains_key(&key) {
                if let Some(existing) = self.states.get(&key) {
                    let mut checksums = Vec::new();
                    checksums.try_reserve_exact(existing.checksums.len())
                        .map_err(|_| Error::Execution(
                            "shuffle chunk staging allocation failed".into()
                        ))?;
                    checksums.extend_from_slice(&existing.checksums);
                    staged.states.try_reserve(1)
                        .map_err(|_| Error::Execution(
                            "shuffle partition staging allocation failed".into()
                        ))?;
                    staged.states.insert(key, PartitionState {
                        attempt: existing.attempt,
                        checksums,
                        committed: existing.committed,
                    });
                }
            }
            accepted.push(staged.record_chunk(
                chunk.stage, chunk.partition, chunk.attempt,
                chunk.sequence, chunk.checksum
            )?);
        }

        let new_partitions = staged.states.keys()
            .filter(|&&key| !self.states.contains_key(&key)).count();
        if new_partitions > self.max_partitions.saturating_sub(self.states.len()) {
            return Err(Error::Execution("shuffle partition tracking budget exceeded".into()));
        }
        // Reserve before the first mutation to keep failure atomic.
        self.states.try_reserve(new_partitions)
            .map_err(|_| Error::Execution("shuffle partition index allocation failed".into()))?;
        self.states.extend(staged.states);
        Ok(accepted)
    }

    /// Commit a partition only after receiving the complete ordered sequence.
    /// Stale and repeated attempts return false without changing the ledger.
    pub fn commit(
        &mut self, stage: u64, partition: u64, attempt: u32,
        total_chunks: usize,
    ) -> Result<bool> {
        let state = self.states.get_mut(&(stage, partition))
            .ok_or_else(|| Error::Execution("unknown shuffle partition commit".into()))?;
        if attempt != state.attempt { return Ok(false); }
        if state.committed { return Ok(false); }
        if state.checksums.len() != total_chunks {
            return Err(Error::Execution("shuffle partition has incomplete chunks".into()));
        }
        state.committed = true;
        Ok(true)
    }

    pub fn is_committed(&self, stage: u64, partition: u64) -> bool {
        self.states.get(&(stage, partition))
            .is_some_and(|state| state.committed)
    }

    pub fn tracked_partitions(&self) -> usize { self.states.len() }
}
