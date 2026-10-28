//! Query-scoped memory accounting independent of DataFusion.
//! Reservations release their bytes when dropped, including on error paths.
use std::sync::{Arc, Mutex};

use crate::{Error, Result};

#[derive(Debug)]
struct Usage {
    limit: u64,
    used: u64,
}

/// Shared budget for operators executing a single query.
#[derive(Clone, Debug)]
pub struct MemoryPool {
    inner: Arc<Mutex<Usage>>,
}

#[derive(Debug)]
pub struct MemoryReservation {
    pool: MemoryPool,
    bytes: u64,
}

impl MemoryPool {
    pub fn new(limit: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Usage { limit, used: 0 })),
        }
    }

    pub fn limit(&self) -> u64 {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).limit
    }

    pub fn used(&self) -> u64 {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).used
    }

    pub fn reserve(&self, bytes: u64) -> Result<MemoryReservation> {
        {
            let mut usage = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let next = usage.used.checked_add(bytes)
                .ok_or_else(|| Error::Execution("memory accounting overflow".into()))?;
            if next > usage.limit {
                return Err(Error::Execution(format!(
                    "native query memory limit exceeded (requested {bytes}, used {}, limit {})",
                    usage.used, usage.limit
                )));
            }
            usage.used = next;
        }
        Ok(MemoryReservation { pool: self.clone(), bytes })
    }
}

impl MemoryReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Grow or shrink atomically without losing the existing reservation on failure.
    pub fn resize(&mut self, new_bytes: u64) -> Result<()> {
        let mut usage = self.pool.inner.lock().unwrap_or_else(|e| e.into_inner());
        if new_bytes >= self.bytes {
            let delta = new_bytes - self.bytes;
            let next = usage.used.checked_add(delta)
                .ok_or_else(|| Error::Execution("memory accounting overflow".into()))?;
            if next > usage.limit {
                return Err(Error::Execution("native query memory limit exceeded".into()));
            }
            usage.used = next;
        } else {
            usage.used -= self.bytes - new_bytes;
        }
        self.bytes = new_bytes;
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        let mut usage = self.pool.inner.lock().unwrap_or_else(|e| e.into_inner());
        usage.used -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_grow_shrink_and_release() {
        let pool = MemoryPool::new(100);
        let mut left = pool.reserve(40).unwrap();
        let right = pool.reserve(30).unwrap();
        assert!(left.resize(80).is_err());
        assert_eq!(left.bytes(), 40);
        assert_eq!(pool.used(), 70);
        left.resize(20).unwrap();
        assert_eq!(pool.used(), 50);
        drop(right);
        drop(left);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn concurrent_reservations_never_overcommit() {
        let pool = MemoryPool::new(10);
        std::thread::scope(|scope| {
            let first = pool.reserve(10).unwrap();
            scope.spawn(|| assert!(pool.reserve(1).is_err())).join().unwrap();
            drop(first);
        });
        assert!(pool.reserve(10).is_ok());
    }

    #[test]
    fn overflow_and_zero_budgets_are_handled() {
        let pool = MemoryPool::new(u64::MAX);
        let _all = pool.reserve(u64::MAX).unwrap();
        assert!(pool.reserve(1).is_err());
        let empty = MemoryPool::new(0);
        assert!(empty.reserve(0).is_ok());
        assert!(empty.reserve(1).is_err());
    }
}
