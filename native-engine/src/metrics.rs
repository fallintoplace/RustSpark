use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use crate::{Error, Result};

#[derive(Debug, Default)]
struct Counters {
    input_rows: AtomicU64,
    output_rows: AtomicU64,
    input_batches: AtomicU64,
    output_batches: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub struct QueryMetrics(Arc<Counters>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub input_rows: u64,
    pub output_rows: u64,
    pub input_batches: u64,
    pub output_batches: u64,
}

fn charge(counter: &AtomicU64, amount: u64) -> Result<()> {
    counter.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
        current.checked_add(amount)
    }).map_err(|_| Error::Execution("native query metric overflow".into()))?;
    Ok(())
}

fn charge_event(rows: &AtomicU64, batches: &AtomicU64, amount: u64) -> Result<()> {
    // Reserve the event counter first, so a failed row charge can be rolled back.
    charge(batches, 1)?;
    if let Err(error) = charge(rows, amount) {
        batches.fetch_sub(1, Ordering::AcqRel);
        return Err(error);
    }
    Ok(())
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_input(&self, rows: u64) -> Result<()> {
        charge_event(&self.0.input_rows, &self.0.input_batches, rows)
    }

    pub fn record_output(&self, rows: u64) -> Result<()> {
        charge_event(&self.0.output_rows, &self.0.output_batches, rows)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            input_rows: self.0.input_rows.load(Ordering::Acquire),
            output_rows: self.0.output_rows.load(Ordering::Acquire),
            input_batches: self.0.input_batches.load(Ordering::Acquire),
            output_batches: self.0.output_batches.load(Ordering::Acquire),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_events_do_not_partially_increment_counters() {
        let metrics = QueryMetrics::new();
        metrics.0.input_rows.store(u64::MAX, Ordering::Release);
        assert!(metrics.record_input(1).is_err());
        assert_eq!(metrics.snapshot().input_batches, 0);
        assert_eq!(metrics.snapshot().input_rows, u64::MAX);

        metrics.0.input_rows.store(0, Ordering::Release);
        metrics.0.input_batches.store(u64::MAX, Ordering::Release);
        assert!(metrics.record_input(1).is_err());
        assert_eq!(metrics.snapshot().input_rows, 0);

        metrics.0.output_rows.store(u64::MAX, Ordering::Release);
        assert!(metrics.record_output(1).is_err());
        assert_eq!(metrics.snapshot().output_batches, 0);
    }

    #[test]
    fn snapshots_include_metrics_from_clones() {
        let metrics = QueryMetrics::new();
        let worker = metrics.clone();
        worker.record_input(100).unwrap();
        metrics.record_output(7).unwrap();
        assert_eq!(metrics.snapshot(), Snapshot {
            input_rows: 100, output_rows: 7,
            input_batches: 1, output_batches: 1,
        });
    }
}

impl Snapshot {
    /// Combine independent stage snapshots, rejecting counter overflow.
    pub fn merge(self, other: Self) -> Result<Self> {
        let add = |a: u64, b: u64| a.checked_add(b)
            .ok_or_else(|| Error::Execution("merged metric overflow".into()));
        Ok(Self {
            input_rows: add(self.input_rows, other.input_rows)?,
            output_rows: add(self.output_rows, other.output_rows)?,
            input_batches: add(self.input_batches, other.input_batches)?,
            output_batches: add(self.output_batches, other.output_batches)?,
        })
    }
}

#[cfg(test)]
mod metrics_merge_feature_tests {
    use super::*;

    #[test]
    fn snapshots_are_summed_and_overflow_is_rejected() {
        let first = Snapshot { input_rows: 100, output_rows: 20, input_batches: 3, output_batches: 1 };
        let second = Snapshot { input_rows: 5, output_rows: 2, input_batches: 1, output_batches: 1 };
        assert_eq!(first.merge(second).unwrap(), Snapshot {
            input_rows: 105, output_rows: 22, input_batches: 4, output_batches: 2,
        });
        assert!(Snapshot { input_rows: u64::MAX, ..first }.merge(second).is_err());
    }
}
