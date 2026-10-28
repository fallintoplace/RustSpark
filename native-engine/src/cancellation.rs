//! Cooperative cancellation for independent native query operators.
use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::{Duration, Instant},
};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct QueryCancellation {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl QueryCancellation {
    pub fn new() -> Self {
        Self { cancelled: Arc::new(AtomicBool::new(false)), deadline: None }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let mut context = Self::new();
        // A deadline overflow cannot be reached in practice; fall back to
        // the largest supported (unbounded) deadline rather than panicking.
        context.deadline = Instant::now().checked_add(timeout);
        context
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self { cancelled: Arc::new(AtomicBool::new(false)), deadline: Some(deadline) }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Called at scan, join, aggregate, and pipeline boundaries.
    pub fn checkpoint(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(Error::Execution("native query cancelled".into()));
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::Execution("native query deadline exceeded".into()));
        }
        Ok(())
    }
}

impl Default for QueryCancellation {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_visible_across_worker_threads() {
        let guard = QueryCancellation::new();
        let other = guard.clone();
        std::thread::spawn(move || other.cancel()).join().unwrap();
        assert!(guard.is_cancelled());
        assert!(guard.checkpoint().is_err());
    }

    #[test]
    fn expired_deadline_fails_without_sleeping() {
        assert!(QueryCancellation::with_deadline(Instant::now()).checkpoint().is_err());
        assert!(QueryCancellation::with_timeout(Duration::from_secs(3600)).checkpoint().is_ok());
    }
}
