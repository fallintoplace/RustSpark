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
    if amount == 0 { return Ok(()); }
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
    fn combine_checked(
        self, other: Self, operation: impl Fn(u64, u64) -> Result<u64>,
    ) -> Result<Self> {
        Ok(Self {
            input_rows: operation(self.input_rows, other.input_rows)?,
            output_rows: operation(self.output_rows, other.output_rows)?,
            input_batches: operation(self.input_batches, other.input_batches)?,
            output_batches: operation(self.output_batches, other.output_batches)?,
        })
    }

    /// Derive nonnegative metric changes since an earlier snapshot.
    pub fn delta_since(self, earlier: Self) -> Result<Self> {
        self.combine_checked(earlier, |current, old| current.checked_sub(old)
            .ok_or_else(|| Error::Execution("query metrics moved backwards".into())))
    }

    /// Combine independent stage snapshots, rejecting counter overflow.
    pub fn merge(self, other: Self) -> Result<Self> {
        self.combine_checked(other, |a, b| a.checked_add(b)
            .ok_or_else(|| Error::Execution("merged metric overflow".into())))
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

#[cfg(test)]
mod snapshot_metric_deltas_tests {
    use super::*;

    #[test]
    fn computes_interval_deltas_and_rejects_counter_regressions() {
        let metrics = QueryMetrics::new();
        metrics.record_input(100).unwrap();
        let before = metrics.snapshot();
        metrics.record_input(25).unwrap();
        metrics.record_output(15).unwrap();
        let after = metrics.snapshot();
        assert_eq!(after.delta_since(before).unwrap(), Snapshot {
            input_rows: 25, output_rows: 15, input_batches: 1, output_batches: 1,
        });
        assert!(before.delta_since(after).is_err());
    }
}

#[cfg(test)]
mod zero_row_metrics_fast_path_tests {
    use super::*;
    #[test]
    fn empty_batches_increment_only_batch_counters() {
        let metrics = QueryMetrics::new();
        metrics.record_input(0).unwrap();
        metrics.record_output(0).unwrap();
        assert_eq!(metrics.snapshot(), Snapshot {
            input_rows: 0, output_rows: 0,
            input_batches: 1, output_batches: 1,
        });
        metrics.0.input_rows.store(u64::MAX, Ordering::Release);
        metrics.record_input(0).unwrap();
        assert_eq!(metrics.snapshot().input_batches, 2);
    }
}
