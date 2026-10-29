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

    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        self.charge(bytes)?;
        Ok(Reservation {
            pool: Arc::clone(self),
            bytes,
        })
    }

    fn charge(&self, bytes: usize) -> Result<()> {
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

    /// Transfer part of an existing reservation without changing total pool usage.
    pub fn split_off(&mut self, bytes: usize) -> Result<Self> {
        if bytes > self.bytes {
            return Err(Error::Execution("cannot split more memory than reserved".into()));
        }
        self.bytes -= bytes;
        Ok(Self { pool: Arc::clone(&self.pool), bytes })
    }

    pub fn resize(&mut self, new_bytes: usize) -> Result<()> {
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
