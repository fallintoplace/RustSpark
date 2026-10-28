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

impl QueryMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_input(&self, rows: u64) -> Result<()> {
        charge(&self.0.input_rows, rows)?;
        charge(&self.0.input_batches, 1)
    }

    pub fn record_output(&self, rows: u64) -> Result<()> {
        charge(&self.0.output_rows, rows)?;
        charge(&self.0.output_batches, 1)
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
