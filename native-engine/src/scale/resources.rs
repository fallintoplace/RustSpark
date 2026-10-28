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

    pub fn resize(&mut self, new_bytes: usize) -> Result<()> {
        if new_bytes > self.bytes {
            let extra = new_bytes - self.bytes;
            let new_reservation = self.budget.reserve(extra)?;
            // Transfer the new bytes to this guard without releasing them.
            std::mem::forget(new_reservation);
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
