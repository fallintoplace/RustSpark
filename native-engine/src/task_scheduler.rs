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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TaskCounts {
    pub queued: usize,
    pub leased: usize,
    pub done: usize,
    pub failed: usize,
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
        self.finish_at(lease, success, Instant::now())
    }

    pub fn finish_at(&mut self, lease: &Lease, success: bool, now: Instant) -> bool {
        let Some(task) = self.tasks.get_mut(&lease.task) else {
            return false;
        };
        let until = match &task.status {
            TaskStatus::Leased { worker, attempt, until }
                if *worker == lease.worker && *attempt == lease.attempt => *until,
            _ => return false,
        };
        if now >= until {
            task.status = if task.attempts >= self.max_attempts {
                TaskStatus::Failed
            } else {
                TaskStatus::Queued
            };
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

    pub fn cancel_task(&mut self, id: u64) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else { return false; };
        if matches!(task.status, TaskStatus::Queued | TaskStatus::Leased { .. }) {
            task.status = TaskStatus::Failed;
            return true;
        }
        false
    }

    pub fn counts(&self) -> TaskCounts {
        let mut counts = TaskCounts::default();
        for task in self.tasks.values() {
            match &task.status {
                TaskStatus::Queued => counts.queued += 1,
                TaskStatus::Leased { .. } => counts.leased += 1,
                TaskStatus::Done => counts.done += 1,
                TaskStatus::Failed => counts.failed += 1,
            }
        }
        counts
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
    fn task_counts_track_queue_and_terminal_states() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1, 2, 3], 2).unwrap();
        assert_eq!(scheduler.counts(), TaskCounts { queued: 3, ..TaskCounts::default() });
        let first = scheduler.lease(7, now, Duration::from_secs(3)).unwrap().unwrap();
        assert_eq!(scheduler.counts(), TaskCounts { queued: 2, leased: 1, ..TaskCounts::default() });
        assert!(scheduler.finish(&first, true));
        assert!(scheduler.cancel_task(2));
        assert_eq!(scheduler.counts(), TaskCounts { queued: 1, done: 1, failed: 1, ..TaskCounts::default() });
    }

    #[test]
    fn targeted_cancellation_invalidates_only_the_selected_lease() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1, 2, 3], 3).unwrap();
        let first = scheduler.lease(7, now, Duration::from_secs(5)).unwrap().unwrap();
        assert!(scheduler.cancel_task(first.task));
        assert!(!scheduler.finish(&first, true));
        assert!(!scheduler.cancel_task(first.task));
        assert!(!scheduler.cancel_task(99));
        let second = scheduler.lease(9, now, Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(second.task, 2);
        assert!(scheduler.finish(&second, true));
        assert!(scheduler.cancel_task(3));
        assert_eq!(scheduler.status(2), Some(&TaskStatus::Done));
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

 
impl TaskScheduler {
    /// Fail queued and leased tasks, preventing any late attempt from committing.
    pub fn cancel_pending(&mut self) -> usize {
        let mut cancelled = 0;
        for task in self.tasks.values_mut() {
            if matches!(task.status, TaskStatus::Queued | TaskStatus::Leased { .. }) {
                task.status = TaskStatus::Failed;
                cancelled += 1;
            }
        }
        cancelled
    }
}

#[cfg(test)]
mod task_cancel_feature_tests {
    use super::*;

    #[test]
    fn cancellation_invalidates_inflight_leases() {
        let mut scheduler = TaskScheduler::new([1, 2], 3).unwrap();
        let lease = scheduler.lease(9, Instant::now(), Duration::from_secs(30)).unwrap().unwrap();
        assert_eq!(scheduler.cancel_pending(), 2);
        assert!(!scheduler.finish(&lease, true));
        assert_eq!(scheduler.lease(9, Instant::now(), Duration::from_secs(30)).unwrap(), None);
        assert_eq!(scheduler.cancel_pending(), 0);
    }
}


#[cfg(test)]
mod expired_finish_tests {
    use super::*;

    #[test]
    fn expired_attempt_cannot_commit_even_without_another_lease() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([7], 2).unwrap();
        let first = scheduler.lease(11, now, Duration::from_secs(1)).unwrap().unwrap();
        assert!(!scheduler.finish_at(&first, true, now + Duration::from_secs(2)));
        assert_eq!(scheduler.status(7), Some(&TaskStatus::Queued));
        let retry = scheduler.lease(12, now + Duration::from_secs(2), Duration::from_secs(1)).unwrap().unwrap();
        assert!(scheduler.finish_at(&retry, true, now + Duration::from_secs(2)));
        assert_eq!(scheduler.status(7), Some(&TaskStatus::Done));
    }
}
