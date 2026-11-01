use std::{
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::{Duration, Instant},
};

use crate::{Error, Result};

#[derive(Clone, Debug, Default)]
pub struct Cancellation {
    cancelled: Arc<AtomicBool>,
}

impl Cancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug)]
pub struct QueryGuard {
    cancellation: Cancellation,
    deadline: Option<Instant>,
}

impl QueryGuard {
    pub fn new(cancellation: Cancellation) -> Self {
        Self { cancellation, deadline: None }
    }

    pub fn with_deadline(cancellation: Cancellation, deadline: Instant) -> Self {
        Self { cancellation, deadline: Some(deadline) }
    }

    pub fn with_timeout(cancellation: Cancellation, timeout: Duration) -> Result<Self> {
        let deadline = Instant::now().checked_add(timeout)
            .ok_or_else(|| Error::Execution("query timeout overflow".into()))?;
        Ok(Self { cancellation, deadline: Some(deadline) })
    }

    /// Create a child execution guard that shares cancellation and inherits
    /// the earlier of the parent deadline and a new local timeout.
    pub fn child_with_timeout(&self, timeout: Duration) -> Result<Self> {
        let local = Instant::now().checked_add(timeout)
            .ok_or_else(|| Error::Execution("query timeout overflow".into()))?;
        let deadline = self.deadline.map_or(local, |parent| parent.min(local));
        Ok(Self { cancellation: self.cancellation.clone(), deadline: Some(deadline) })
    }

    /// Monotonic time left before the deadline, or None for unbounded queries.
    pub fn remaining_time(&self) -> Option<Duration> {
        self.deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(Error::Execution("query cancelled".into()));
        }
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(Error::Execution("query deadline exceeded".into()));
        }
        Ok(())
    }
}

impl Default for QueryGuard {
    fn default() -> Self {
        Self::new(Cancellation::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_shared_with_all_clones() {
        let token = Cancellation::new();
        let guard = QueryGuard::new(token.clone());
        assert!(guard.check().is_ok());
        token.cancel();
        assert!(guard.check().unwrap_err().to_string().contains("cancelled"));
    }

    #[test]
    fn deadlines_are_enforced_without_sleeping() {
        let guard = QueryGuard::with_timeout(Cancellation::new(), Duration::ZERO).unwrap();
        assert!(guard.check().unwrap_err().to_string().contains("deadline"));
    }
}

#[cfg(test)]
mod absolute_deadline_engine_tests {
    use super::*;

    #[test]
    fn original_cancellation_and_deadline_are_shared() {
        let token = Cancellation::new();
        let guard = QueryGuard::with_deadline(token.clone(), Instant::now());
        assert!(guard.check().unwrap_err().to_string().contains("deadline"));
        token.cancel();
        assert!(guard.check().unwrap_err().to_string().contains("cancelled"));
    }
}

#[cfg(test)]
mod query_guard_remaining_deadline_tests {
    use super::*;

    #[test]
    fn reports_bounded_and_unbounded_remaining_time() {
        let unbounded = QueryGuard::default();
        assert_eq!(unbounded.remaining_time(), None);
        let expired = QueryGuard::with_deadline(Cancellation::new(), Instant::now());
        assert_eq!(expired.remaining_time(), Some(Duration::ZERO));
        let bounded = QueryGuard::with_timeout(
            Cancellation::new(), Duration::from_secs(60)).unwrap();
        let remaining = bounded.remaining_time().unwrap();
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::ZERO);
    }
}

#[cfg(test)]
mod scoped_query_guard_tests {
    use super::*;

    #[test]
    fn nested_timeouts_cannot_extend_the_parent_and_share_cancellation() {
        let parent = QueryGuard::with_timeout(
            Cancellation::new(), Duration::from_secs(30)).unwrap();
        let child = parent.child_with_timeout(Duration::from_secs(1)).unwrap();
        let grandchild = child.child_with_timeout(Duration::from_secs(60)).unwrap();
        assert!(child.deadline.unwrap() <= parent.deadline.unwrap());
        assert_eq!(grandchild.deadline, child.deadline);
        assert!(grandchild.check().is_ok());
        child.cancellation.cancel();
        assert!(parent.check().unwrap_err().to_string().contains("cancelled"));
        assert!(grandchild.check().is_err());
    }

    #[test]
    fn expired_parent_stays_expired_and_invalid_timeout_rejects() {
        let parent = QueryGuard::with_deadline(Cancellation::new(), Instant::now());
        let child = parent.child_with_timeout(Duration::from_secs(600)).unwrap();
        assert!(child.check().unwrap_err().to_string().contains("deadline"));
        assert!(QueryGuard::default().child_with_timeout(Duration::MAX).is_err());
        let unbounded = QueryGuard::default();
        assert!(unbounded.child_with_timeout(Duration::from_secs(5)).unwrap().check().is_ok());
    }
}
