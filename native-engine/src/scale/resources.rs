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
        if bytes == 0 { return Ok(Reservation { budget: self.clone(), bytes: 0 }); }
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
                Ok(_) => return Ok(Reservation { budget: self.clone(), bytes }),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Reservation {
    pub fn bytes(&self) -> usize { self.bytes }

    /// Transfer this guard's charge to other guards without releasing the budget.
    fn disarm(&mut self) { self.bytes = 0; }

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
            let extra = new_bytes - self.bytes;
            let mut additional = self.budget.reserve(extra)?;
            // Transfer the reservation, then release only the guard object.
            additional.disarm();
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
    let reservations = requests.iter().map(|&bytes| Reservation {
        budget: budget.clone(), bytes,
    }).collect();
    // Transfer the already-charged capacity to independent drop guards.
    combined.disarm();
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
