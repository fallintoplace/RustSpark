//! Cooperative cancellation and monotonic query deadlines.
use std::time::{Duration, Instant};
use crate::{cancel::Cancellation, Error, Result};

#[derive(Clone, Debug)]
pub struct QueryControl {
    cancelled: Cancellation,
    deadline: Option<Instant>,
}

impl QueryControl {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self { cancelled: Cancellation::new(), deadline }
    }

    pub fn with_timeout(timeout: Duration) -> Result<Self> {
        let deadline = Instant::now().checked_add(timeout)
            .ok_or_else(|| Error::Execution("query deadline out of range".into()))?;
        Ok(Self::new(Some(deadline)))
    }

    pub fn cancel(&self) {
        self.cancelled.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }

    /// Remaining monotonic time until the query deadline, if configured.
    pub fn remaining_time(&self) -> Option<Duration> {
        self.deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
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

#[cfg(test)]
mod remaining_deadline_tests {
    use super::*;

    #[test]
    fn reports_remaining_and_expired_deadlines() {
        assert_eq!(QueryControl::default().remaining_time(), None);
        let expired = QueryControl::new(Some(Instant::now()));
        assert_eq!(expired.remaining_time(), Some(Duration::ZERO));
        let live = QueryControl::with_timeout(Duration::from_secs(60)).unwrap();
        let remaining = live.remaining_time().unwrap();
        assert!(remaining > Duration::ZERO && remaining <= Duration::from_secs(60));
        live.cancel();
        assert!(live.check().is_err());
    }
}
