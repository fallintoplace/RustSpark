//! Cooperative cancellation and monotonic query deadlines.
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::{Duration, Instant};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct QueryControl {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl QueryControl {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self { cancelled: Arc::new(AtomicBool::new(false)), deadline }
    }

    pub fn with_timeout(timeout: Duration) -> Result<Self> {
        let deadline = Instant::now().checked_add(timeout)
            .ok_or_else(|| Error::Execution("query deadline out of range".into()))?;
        Ok(Self::new(Some(deadline)))
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(Error::Execution("query cancelled".into()));
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::Execution("query deadline exceeded".into()));
        }
        Ok(())
    }
}

impl Default for QueryControl {
    fn default() -> Self { Self::new(None) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_workers_observe_cancellation() {
        let control = QueryControl::default();
        let worker = control.clone();
        assert!(worker.check().is_ok());
        control.cancel();
        assert!(worker.is_cancelled());
        assert!(worker.check().unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn zero_timeout_has_expired() {
        let control = QueryControl::with_timeout(Duration::ZERO).unwrap();
        assert!(control.check().unwrap_err().to_string().contains("deadline"));
    }
}
