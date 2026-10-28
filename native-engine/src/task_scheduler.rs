use std::{collections::BTreeMap, time::{Duration, Instant}};

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Leased { worker: u64, attempt: u32, until: Instant },
    Done,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub task: u64,
    pub worker: u64,
    pub attempt: u32,
}

struct Task {
    attempts: u32,
    status: TaskStatus,
}

pub struct TaskScheduler {
    tasks: BTreeMap<u64, Task>,
    max_attempts: u32,
}

impl TaskScheduler {
    pub fn new(tasks: impl IntoIterator<Item = u64>, max_attempts: u32) -> Result<Self> {
        if max_attempts == 0 {
            return Err(Error::Execution("task retry budget must be positive".into()));
        }
        let mut entries = BTreeMap::new();
        for id in tasks {
            if entries.insert(id, Task {
                attempts: 0, status: TaskStatus::Queued,
            }).is_some() {
                return Err(Error::Execution("duplicate task ID".into()));
            }
        }
        Ok(Self { tasks: entries, max_attempts })
    }

    pub fn lease(&mut self, worker: u64, now: Instant, ttl: Duration) -> Result<Option<Lease>> {
        if ttl.is_zero() {
            return Err(Error::Execution("task lease duration must be positive".into()));
        }
        let until = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task lease deadline overflow".into()))?;
        for task in self.tasks.values_mut() {
            if matches!(&task.status, TaskStatus::Leased { until, .. } if *until <= now) {
                task.status = if task.attempts >= self.max_attempts {
                    TaskStatus::Failed
                } else {
                    TaskStatus::Queued
                };
            }
        }
        for (&id, task) in &mut self.tasks {
            if task.status == TaskStatus::Queued {
                task.attempts += 1;
                task.status = TaskStatus::Leased { worker, attempt: task.attempts, until };
                return Ok(Some(Lease { task: id, worker, attempt: task.attempts }));
            }
        }
        Ok(None)
    }

    pub fn finish(&mut self, lease: &Lease, success: bool) -> bool {
        let Some(task) = self.tasks.get_mut(&lease.task) else {
            return false;
        };
        if !matches!(&task.status, TaskStatus::Leased { worker, attempt, .. }
            if *worker == lease.worker && *attempt == lease.attempt) {
            return false;
        }
        task.status = if success {
            TaskStatus::Done
        } else if task.attempts >= self.max_attempts {
            TaskStatus::Failed
        } else {
            TaskStatus::Queued
        };
        true
    }

    pub fn status(&self, task: u64) -> Option<&TaskStatus> {
        self.tasks.get(&task).map(|entry| &entry.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_out_attempts_cannot_commit_late_results() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1, 2], 2).unwrap();
        let original = scheduler.lease(7, now, Duration::from_secs(1)).unwrap().unwrap();
        let replacement = scheduler.lease(8, now + Duration::from_secs(2), Duration::from_secs(1))
            .unwrap().unwrap();
        assert_eq!(original.task, replacement.task);
        assert_ne!(original.attempt, replacement.attempt);
        assert!(!scheduler.finish(&original, true));
        assert!(scheduler.finish(&replacement, true));
        assert_eq!(scheduler.status(1), Some(&TaskStatus::Done));
    }

    #[test]
    fn retry_exhaustion_marks_failed_tasks() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([4], 1).unwrap();
        let lease = scheduler.lease(1, now, Duration::from_secs(1)).unwrap().unwrap();
        assert!(scheduler.finish(&lease, false));
        assert_eq!(scheduler.status(4), Some(&TaskStatus::Failed));
        assert_eq!(scheduler.lease(1, now, Duration::from_secs(1)).unwrap(), None);
    }
}
