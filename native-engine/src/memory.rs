use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{Error, Result};

#[derive(Debug)]
pub struct MemoryPool {
    limit: usize,
    used: AtomicUsize,
}

impl MemoryPool {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicUsize::new(0),
        })
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    /// Snapshot of unreserved capacity. Concurrent reservations may change it.
    pub fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.used())
    }

    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        self.charge(bytes)?;
        Ok(Reservation {
            pool: Arc::clone(self),
            bytes,
        })
    }

    /// Atomically claim at most the requested number of free bytes.
    /// Callers can shrink work units when less capacity is available.
    pub fn reserve_up_to(self: &Arc<Self>, requested: usize) -> Result<Reservation> {
        if requested == 0 {
            return Ok(Reservation { pool: Arc::clone(self), bytes: 0 });
        }
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let available = self.limit.saturating_sub(current);
            // Do not write a full pool's cache line for zero-byte claims.
            // The snapshot is an allowed linearization point for claiming
            // no capacity, and a later caller can retry after releases.
            if available == 0 {
                return Ok(Reservation { pool: Arc::clone(self), bytes: 0 });
            }
            let bytes = requested.min(available);
            let next = current.checked_add(bytes)
                .ok_or_else(|| Error::Execution("memory reservation overflow".into()))?;
            match self.used.compare_exchange_weak(
                current, next, Ordering::AcqRel, Ordering::Relaxed
            ) {
                Ok(_) => return Ok(Reservation { pool: Arc::clone(self), bytes }),
                Err(observed) => current = observed,
            }
        }
    }

    fn charge(&self, bytes: usize) -> Result<()> {
        if bytes == 0 { return Ok(()); }
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or_else(|| Error::Execution("memory reservation overflow".into()))?;
            if next > self.limit {
                return Err(Error::Execution(format!(
                    "native query memory budget exceeded: requested {bytes} bytes with {current} of {} already reserved",
                    self.limit
                )));
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Debug)]
pub struct Reservation {
    pool: Arc<MemoryPool>,
    bytes: usize,
}

impl Reservation {
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Combine owned reservations without releasing and reacquiring pool
    /// capacity. Only guards from the same memory pool can be combined.
    /// A failed transfer leaves both guards and accounting unchanged.
    pub fn absorb(&mut self, other: &mut Self) -> Result<()> {
        if !Arc::ptr_eq(&self.pool, &other.pool) {
            return Err(Error::Execution("cannot combine different memory pools".into()));
        }
        let total = self.bytes.checked_add(other.bytes)
            .ok_or_else(|| Error::Execution("memory reservation overflow".into()))?;
        self.bytes = total;
        other.bytes = 0;
        Ok(())
    }

    /// Transfer part of an existing reservation without changing total pool usage.
    pub fn split_off(&mut self, bytes: usize) -> Result<Self> {
        if bytes > self.bytes {
            return Err(Error::Execution("cannot split more memory than reserved".into()));
        }
        self.bytes -= bytes;
        Ok(Self { pool: Arc::clone(&self.pool), bytes })
    }

    pub fn resize(&mut self, new_bytes: usize) -> Result<()> {
        if new_bytes == self.bytes { return Ok(()); }
        if new_bytes > self.bytes {
            let extra = new_bytes - self.bytes;
            self.pool.charge(extra)?;
        } else {
            self.pool.used.fetch_sub(self.bytes - new_bytes, Ordering::AcqRel);
        }
        self.bytes = new_bytes;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn reservations_are_released_and_resize_is_accounted() {
        let pool = MemoryPool::new(100);
        let mut first = pool.reserve(60).unwrap();
        assert!(pool.reserve(41).is_err());
        first.resize(80).unwrap();
        assert_eq!(pool.used(), 80);
        assert!(first.resize(101).is_err());
        assert_eq!(first.bytes(), 80);
        first.resize(12).unwrap();
        assert_eq!(pool.used(), 12);
        drop(first);
        assert_eq!(pool.used(), 0);
        assert!(pool.reserve(100).is_ok());
    }

    #[test]
    fn threads_cannot_overcommit_the_pool() {
        let pool = MemoryPool::new(10);
        let _held = pool.reserve(9).unwrap();
        thread::scope(|scope| {
            for _ in 0..8 {
                let pool = Arc::clone(&pool);
                scope.spawn(move || assert!(pool.reserve(2).is_err()));
            }
        });
        assert_eq!(pool.used(), 9);
    }

    #[test]
    fn overflow_fails_without_charging() {
        let pool = MemoryPool::new(usize::MAX);
        let _held = pool.reserve(usize::MAX).unwrap();
        assert!(pool.reserve(1).is_err());
        assert_eq!(pool.used(), usize::MAX);
    }
}

 
/// Reserve a group of budgets with one atomic pool charge.
pub fn reserve_many(pool: &Arc<MemoryPool>, requests: &[usize]) -> Result<Vec<Reservation>> {
    let total = requests.iter().try_fold(0usize, |sum, &bytes| {
        sum.checked_add(bytes)
            .ok_or_else(|| Error::Execution("memory reservation overflow".into()))
    })?;
    let mut combined = pool.reserve(total)?;
    let mut reservations = Vec::with_capacity(requests.len());
    for &bytes in requests {
        reservations.push(combined.split_off(bytes)?);
    }
    debug_assert_eq!(combined.bytes(), 0);
    Ok(reservations)
}

/// Claim available pool capacity once and distribute the grant among requests
/// in priority order. Concurrent pool users never see intermediate charges.
/// Returns zero-byte guards when no budget remains, with no overcommit.
pub fn reserve_many_up_to(
    pool: &Arc<MemoryPool>, requests: &[usize],
) -> Result<Vec<Reservation>> {
    let requested = requests.iter().try_fold(0usize, |total, &bytes| {
        total.checked_add(bytes)
            .ok_or_else(|| Error::Execution("memory reservation overflow".into()))
    })?;
    let mut grant = pool.reserve_up_to(requested)?;
    let mut available = grant.bytes();
    let mut reservations = Vec::with_capacity(requests.len());
    for &requested_bytes in requests {
        let bytes = requested_bytes.min(available);
        reservations.push(grant.split_off(bytes)?);
        available -= bytes;
    }
    debug_assert_eq!(grant.bytes(), 0);
    Ok(reservations)
}

#[cfg(test)]
mod bulk_memory_feature_tests {
    use super::*;

    #[test]
    fn partial_failure_returns_all_capacity_to_the_pool() {
        let pool = MemoryPool::new(100);
        assert!(reserve_many(&pool, &[20, 30, 60]).is_err());
        assert_eq!(pool.used(), 0);
        let reservations = reserve_many(&pool, &[40, 60]).unwrap();
        assert_eq!(pool.used(), 100);
        drop(reservations);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn split_reservations_release_exactly_once() {
        let pool = MemoryPool::new(100);
        let mut parent = pool.reserve(75).unwrap();
        let child = parent.split_off(25).unwrap();
        assert_eq!(parent.bytes(), 50);
        assert_eq!(child.bytes(), 25);
        assert_eq!(pool.used(), 75);
        assert!(parent.split_off(51).is_err());
        drop(parent);
        assert_eq!(pool.used(), 25);
        drop(child);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod atomic_reservation_consolidation_tests {
    use super::*;
    #[test]
    fn group_overflow_and_failure_do_not_charge_pool() {
        let pool = MemoryPool::new(100);
        assert!(reserve_many(&pool, &[90, 20]).is_err());
        assert_eq!(pool.used(), 0);
        let allocations = reserve_many(&pool, &[10, 20, 30]).unwrap();
        assert_eq!(pool.used(), 60);
        assert_eq!(allocations.iter().map(Reservation::bytes).sum::<usize>(), 60);
        drop(allocations);
        assert_eq!(pool.used(), 0);
        let big = MemoryPool::new(usize::MAX);
        assert!(reserve_many(&big, &[usize::MAX, 1]).is_err());
        assert_eq!(big.used(), 0);
    }
}

#[cfg(test)]
mod zero_charge_fast_path_tests {
    use super::*;
    #[test]
    fn zero_byte_reservations_work_at_full_budget() {
        let pool = MemoryPool::new(4);
        let _full = pool.reserve(4).unwrap();
        let mut empty = pool.reserve(0).unwrap();
        empty.resize(0).unwrap();
        assert_eq!(pool.used(), 4);
        drop(empty);
        assert_eq!(pool.used(), 4);
    }
}

#[cfg(test)]
mod memory_pool_available_capacity_tests {
    use super::*;

    #[test]
    fn remaining_capacity_tracks_reserve_resize_and_release() {
        let pool = MemoryPool::new(100);
        assert_eq!(pool.remaining(), 100);
        let mut guard = pool.reserve(60).unwrap();
        assert_eq!(pool.remaining(), 40);
        guard.resize(75).unwrap();
        assert_eq!(pool.remaining(), 25);
        assert!(guard.resize(101).is_err());
        assert_eq!(pool.remaining(), 25);
        drop(guard);
        assert_eq!(pool.remaining(), 100);
        let zero = MemoryPool::new(0);
        assert_eq!(zero.remaining(), 0);
    }
}

#[cfg(test)]
mod partial_native_memory_admission_tests {
    use super::*;

    #[test]
    fn allocates_available_capacity_without_overcommitting() {
        let pool = MemoryPool::new(100);
        let first = pool.reserve(70).unwrap();
        let second = pool.reserve_up_to(80).unwrap();
        assert_eq!(second.bytes(), 30);
        assert_eq!(pool.used(), 100);
        let empty = pool.reserve_up_to(20).unwrap();
        assert_eq!(empty.bytes(), 0);
        drop(first);
        assert_eq!(pool.remaining(), 70);
        let next = pool.reserve_up_to(10).unwrap();
        assert_eq!(next.bytes(), 10);
        drop((second, empty, next));
        assert_eq!(pool.used(), 0);
        assert_eq!(MemoryPool::new(0).reserve_up_to(100).unwrap().bytes(), 0);
    }

    #[test]
    fn concurrent_partial_claims_cannot_exceed_capacity() {
        let pool = MemoryPool::new(100);
        let mut reservations = Vec::new();
        std::thread::scope(|scope| {
            let threads = (0..8).map(|_| {
                let pool = Arc::clone(&pool);
                scope.spawn(move || pool.reserve_up_to(30).unwrap())
            }).collect::<Vec<_>>();
            for worker in threads { reservations.push(worker.join().unwrap()); }
        });
        assert_eq!(reservations.iter().map(Reservation::bytes).sum::<usize>(), 100);
        drop(reservations);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod zero_capacity_partial_claim_contention_tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn fully_reserved_pool_returns_empty_guards_without_changing_ownership() {
        let pool = MemoryPool::new(64);
        let full = pool.reserve(64).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let pool = Arc::clone(&pool);
                scope.spawn(move || {
                    for _ in 0..1000 {
                        let claim = pool.reserve_up_to(64).unwrap();
                        assert_eq!(claim.bytes(), 0);
                    }
                });
            }
        });
        assert_eq!(pool.used(), 64);
        assert_eq!(pool.reserve_up_to(0).unwrap().bytes(), 0);
        drop(full);
        let claim = pool.reserve_up_to(64).unwrap();
        assert_eq!(claim.bytes(), 64);
        drop(claim);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod memory_reservation_ownership_transfer_tests {
    use super::*;

    #[test]
    fn combined_guards_do_not_release_or_recharge_capacity() {
        let pool = MemoryPool::new(128);
        let mut first = pool.reserve(40).unwrap();
        let mut second = pool.reserve(50).unwrap();
        first.absorb(&mut second).unwrap();
        assert_eq!(first.bytes(), 90);
        assert_eq!(second.bytes(), 0);
        assert_eq!(pool.used(), 90);
        drop(second);
        assert_eq!(pool.used(), 90);
        let split = first.split_off(60).unwrap();
        assert_eq!(pool.used(), 90);
        first.resize(68).unwrap();
        assert_eq!(pool.used(), 128);
        assert!(first.resize(69).is_err());
        drop(split);
        assert_eq!(pool.used(), 68);
        drop(first);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn cross_pool_absorb_fails_without_changing_either_guard() {
        let first_pool = MemoryPool::new(100);
        let second_pool = MemoryPool::new(100);
        let mut first = first_pool.reserve(20).unwrap();
        let mut other = second_pool.reserve(30).unwrap();
        assert!(first.absorb(&mut other).is_err());
        assert_eq!(first.bytes(), 20);
        assert_eq!(other.bytes(), 30);
        assert_eq!(first_pool.used(), 20);
        assert_eq!(second_pool.used(), 30);
        drop((first, other));
        assert_eq!(first_pool.used(), 0);
        assert_eq!(second_pool.used(), 0);
    }
}

#[cfg(test)]
mod partial_group_memory_reservation_regressions {
    use super::*;
    use std::thread;

    #[test]
    fn group_grant_never_overcommits_and_honors_priority_order() {
        let pool = MemoryPool::new(100);
        let held = pool.reserve(65).unwrap();
        let portions = reserve_many_up_to(&pool, &[10, 40, 20, 0]).unwrap();
        assert_eq!(portions.iter().map(Reservation::bytes).collect::<Vec<_>>(),
            vec![10, 25, 0, 0]);
        assert_eq!(pool.used(), 100);
        assert_eq!(reserve_many_up_to(&pool, &[5, 5]).unwrap()
            .iter().map(Reservation::bytes).collect::<Vec<_>>(), vec![0, 0]);
        drop(held);
        drop(portions);
        assert_eq!(pool.used(), 0);
        assert!(reserve_many_up_to(&pool, &[usize::MAX, 1]).is_err());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn concurrent_partial_claims_remain_linearizable() {
        let pool = MemoryPool::new(101);
        let mut totals = Vec::new();
        thread::scope(|scope| {
            let handles = (0..12).map(|_| {
                let pool = Arc::clone(&pool);
                scope.spawn(move || {
                    let grants = reserve_many_up_to(&pool, &[11, 11]).unwrap();
                    let claimed = grants.iter().map(Reservation::bytes).sum::<usize>();
                    assert!(pool.used() <= pool.limit());
                    (claimed, grants)
                })
            }).collect::<Vec<_>>();
            for handle in handles { totals.push(handle.join().unwrap()); }
        });
        assert!(totals.iter().map(|(claimed, _)| *claimed).sum::<usize>() <= 101);
        assert_eq!(pool.used(), totals.iter().map(|(claimed, _)| *claimed).sum::<usize>());
        drop(totals);
        assert_eq!(pool.used(), 0);
    }
}
