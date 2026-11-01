//! Query-scoped memory accounting without a global allocator or unsafe code.
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct MemoryBudget {
    limit: usize,
    used: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct Reservation {
    budget: MemoryBudget,
    bytes: usize,
}

impl MemoryBudget {
    pub fn new(limit: usize) -> Result<Self> {
        if limit == 0 {
            return Err(Error::Execution("memory budget must be positive".into()));
        }
        Ok(Self { limit, used: Arc::new(AtomicUsize::new(0)) })
    }

    pub fn limit(&self) -> usize { self.limit }

    pub fn used(&self) -> usize { self.used.load(Ordering::Acquire) }

    pub fn remaining(&self) -> usize { self.limit.saturating_sub(self.used()) }

    /// Atomically reserve estimated operator memory. The returned guard releases
    /// the reservation even if the operator exits with an error.
    pub fn reserve(&self, bytes: usize) -> Result<Reservation> {
        self.charge(bytes)?;
        Ok(Reservation { budget: self.clone(), bytes })
    }

    /// Claim as much of the requested capacity as is currently free, without
    /// overcommitting under concurrent claims. A full budget returns a zero-byte
    /// reservation; callers may retry after another guard is released.
    pub fn reserve_up_to(&self, requested: usize) -> Result<Reservation> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            let granted = requested.min(self.limit.saturating_sub(used));
            if granted == 0 {
                return Ok(Reservation { budget: self.clone(), bytes: 0 });
            }
            let next = used.checked_add(granted)
                .ok_or_else(|| Error::Execution("memory reservation overflow".into()))?;
            match self.used.compare_exchange_weak(
                used, next, Ordering::AcqRel, Ordering::Acquire
            ) {
                Ok(_) => return Ok(Reservation { budget: self.clone(), bytes: granted }),
                Err(observed) => used = observed,
            }
        }
    }

    // Charge the existing budget directly. Resizing a reservation must not
    // construct and disarm a second guard for every incremental growth.
    fn charge(&self, bytes: usize) -> Result<()> {
        if bytes == 0 { return Ok(()); }
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)
                .filter(|total| *total <= self.limit)
                .ok_or_else(|| Error::Execution(format!(
                    "query memory budget exceeded: {} requested, {} used, {} limit",
                    bytes, current, self.limit
                )))?;
            match self.used.compare_exchange_weak(
                current, next, Ordering::AcqRel, Ordering::Acquire
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Reservation {
    pub fn bytes(&self) -> usize { self.bytes }

    /// Transfer an already-admitted allocation into another guard without
    /// touching the atomic budget. Cross-budget transfers are not allowed.
    pub fn absorb(&mut self, other: &mut Self) -> Result<()> {
        if !Arc::ptr_eq(&self.budget.used, &other.budget.used) {
            return Err(Error::Execution("cannot combine different memory budgets".into()));
        }
        self.bytes = self.bytes.checked_add(other.bytes)
            .ok_or_else(|| Error::Execution("memory reservation overflow".into()))?;
        other.bytes = 0;
        Ok(())
    }

    /// Transfer part of this allocation to another guard, with no new budget admission.
    pub fn split_off(&mut self, bytes: usize) -> Result<Self> {
        if bytes > self.bytes {
            return Err(Error::Execution("cannot split more memory than reserved".into()));
        }
        self.bytes -= bytes;
        Ok(Self { budget: self.budget.clone(), bytes })
    }

    pub fn resize(&mut self, new_bytes: usize) -> Result<()> {
        if new_bytes > self.bytes {
            self.budget.charge(new_bytes - self.bytes)?;
        } else if new_bytes < self.bytes {
            self.budget.used.fetch_sub(self.bytes - new_bytes, Ordering::AcqRel);
        }
        self.bytes = new_bytes;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn reservations_release_on_drop_and_reject_overcommit() {
        let b = MemoryBudget::new(100).unwrap();
        let mut first = b.reserve(60).unwrap();
        assert!(b.reserve(41).is_err());
        first.resize(80).unwrap();
        assert_eq!(b.remaining(), 20);
        assert!(first.resize(101).is_err());
        first.resize(10).unwrap();
        drop(first);
        assert_eq!(b.used(), 0);
        assert!(MemoryBudget::new(0).is_err());
    }

    #[test]
    fn concurrent_reservations_never_exceed_limit() {
        let b = MemoryBudget::new(100).unwrap();
        let guards: Vec<_> = (0..8).map(|_| {
            let b = b.clone();
            thread::spawn(move || b.reserve(15).ok().map(|g| {
                thread::sleep(std::time::Duration::from_millis(2));
                g.bytes()
            }))
        }).collect();
        let total: usize = guards.into_iter().map(|j| j.join().unwrap().unwrap_or(0)).sum();
        assert!(total <= 120); // concurrent release permits reuse
        assert_eq!(b.used(), 0);
    }
}

/// Reserve a group of operator allocations in a single atomic admission.
pub fn reserve_many(budget: &MemoryBudget, requests: &[usize]) -> Result<Vec<Reservation>> {
    let total = requests.iter().try_fold(0usize, |sum, &bytes| {
        sum.checked_add(bytes)
            .ok_or_else(|| Error::Execution("memory reservation overflow".into()))
    })?;
    let mut combined = budget.reserve(total)?;
    // Transfer ownership incrementally. If collecting the guards unwinds,
    // only the untransferred remainder stays with the parent reservation.
    let mut reservations = Vec::with_capacity(requests.len());
    for &bytes in requests {
        reservations.push(combined.split_off(bytes)?);
    }
    debug_assert_eq!(combined.bytes(), 0);
    Ok(reservations)
}

#[cfg(test)]
mod grouped_scale_memory_tests {
    use super::*;

    #[test]
    fn failed_group_reservation_leaves_budget_uncharged() {
        let budget = MemoryBudget::new(100).unwrap();
        assert!(reserve_many(&budget, &[70, 50]).is_err());
        assert_eq!(budget.used(), 0);
        let allocations = reserve_many(&budget, &[30, 20]).unwrap();
        assert_eq!(budget.used(), 50);
        drop(allocations);
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod scale_reservation_split_tests {
    use super::*;

    #[test]
    fn partial_transfers_release_each_byte_once() {
        let budget = MemoryBudget::new(100).unwrap();
        let mut parent = budget.reserve(80).unwrap();
        let mut child = parent.split_off(25).unwrap();
        assert_eq!(budget.used(), 80);
        assert_eq!(parent.bytes(), 55);
        assert!(parent.split_off(56).is_err());
        child.resize(30).unwrap();
        assert_eq!(budget.used(), 85);
        drop(parent);
        assert_eq!(budget.used(), 30);
        drop(child);
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod zero_scale_reservations_fast_path_tests {
    use super::*;
    #[test]
    fn zero_byte_guards_do_not_consume_full_budget() {
        let budget = MemoryBudget::new(1).unwrap();
        let _full = budget.reserve(1).unwrap();
        let empty = budget.reserve(0).unwrap();
        assert_eq!(empty.bytes(), 0);
        assert_eq!(budget.used(), 1);
        drop(empty);
        assert_eq!(budget.used(), 1);
    }
}

#[cfg(test)]
mod bulk_reservation_transfer_regressions {
    use super::*;

    #[test]
    fn split_guards_keep_exact_budget_accounting_after_resize_and_drop() {
        let budget = MemoryBudget::new(100).unwrap();
        let mut guards = reserve_many(&budget, &[0, 15, 25, 10]).unwrap();
        assert_eq!(budget.used(), 50);
        guards[1].resize(30).unwrap();
        assert_eq!(budget.used(), 65);
        let split = guards[2].split_off(5).unwrap();
        assert_eq!(budget.used(), 65);
        drop(split);
        assert_eq!(budget.used(), 60);
        drop(guards);
        assert_eq!(budget.used(), 0);

        assert!(reserve_many(&budget, &[40, 61]).is_err());
        assert_eq!(budget.used(), 0);
        assert!(reserve_many(&budget, &[]).unwrap().is_empty());
    }
}

#[cfg(test)]
mod direct_budget_resize_regressions {
    use super::*;

    #[test]
    fn repeated_growth_and_failure_preserve_total_reservations() {
        let budget = MemoryBudget::new(1024).unwrap();
        let mut first = budget.reserve(1).unwrap();
        let second = budget.reserve(512).unwrap();
        for target in 2..=400 {
            first.resize(target).unwrap();
            assert_eq!(budget.used(), target + second.bytes());
        }
        assert!(first.resize(513).is_err());
        assert_eq!(first.bytes(), 400);
        assert_eq!(budget.used(), 912);
        first.resize(0).unwrap();
        assert_eq!(budget.used(), 512);
        drop((first, second));
        assert_eq!(budget.used(), 0);
    }
}
#[cfg(test)]
mod reservation_absorb_accounting_regressions {
    use super::*;

    #[test]
    fn combines_guards_without_changing_budget_until_the_owner_drops() {
        let budget = MemoryBudget::new(100).unwrap();
        let mut primary = budget.reserve(30).unwrap();
        let mut secondary = budget.reserve(40).unwrap();
        primary.absorb(&mut secondary).unwrap();
        assert_eq!(primary.bytes(), 70);
        assert_eq!(secondary.bytes(), 0);
        assert_eq!(budget.used(), 70);
        primary.resize(80).unwrap();
        drop(secondary);
        assert_eq!(budget.used(), 80);
        drop(primary);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn never_transfers_a_guard_between_independent_budgets() {
        let first = MemoryBudget::new(64).unwrap();
        let second = MemoryBudget::new(64).unwrap();
        let mut a = first.reserve(20).unwrap();
        let mut b = second.reserve(30).unwrap();
        assert!(a.absorb(&mut b).is_err());
        assert_eq!((a.bytes(), b.bytes()), (20, 30));
        assert_eq!((first.used(), second.used()), (20, 30));
        drop(a);
        drop(b);
        assert_eq!((first.used(), second.used()), (0, 0));
    }
}


#[cfg(test)]
mod adaptive_memory_claim_tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn partial_claims_and_exact_release_never_overcommit() {
        let budget = MemoryBudget::new(100).unwrap();
        let first = budget.reserve(70).unwrap();
        let partial = budget.reserve_up_to(90).unwrap();
        assert_eq!(partial.bytes(), 30);
        assert_eq!(budget.used(), 100);
        let empty = budget.reserve_up_to(50).unwrap();
        assert_eq!(empty.bytes(), 0);
        assert_eq!(budget.used(), 100);
        drop(first);
        assert_eq!(budget.reserve_up_to(15).unwrap().bytes(), 15);
        assert_eq!(budget.used(), 30);
        drop(partial);
        drop(empty);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn concurrent_partial_claims_respect_limit_and_reclaim_every_byte() {
        let budget = MemoryBudget::new(10_003).unwrap();
        let barrier = Arc::new(Barrier::new(16));
        std::thread::scope(|scope| {
            let workers = (0..16).map(|_| {
                let budget = budget.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let reservation = budget.reserve_up_to(10_003).unwrap();
                    assert!(budget.used() <= budget.limit());
                    reservation
                })
            }).collect::<Vec<_>>();
            let reservations = workers.into_iter()
                .map(|thread| thread.join().unwrap()).collect::<Vec<_>>();
            assert_eq!(reservations.iter().map(Reservation::bytes).sum::<usize>(), 10_003);
            assert_eq!(budget.used(), 10_003);
            drop(reservations);
        });
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn zero_request_keeps_a_full_budget_unchanged() {
        let budget = MemoryBudget::new(1).unwrap();
        let full = budget.reserve(1).unwrap();
        let zero = budget.reserve_up_to(0).unwrap();
        assert_eq!(zero.bytes(), 0);
        assert_eq!(budget.used(), 1);
        drop((full, zero));
        assert_eq!(budget.remaining(), 1);
    }
}
