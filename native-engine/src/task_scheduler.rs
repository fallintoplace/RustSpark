use std::{collections::{BTreeMap, BTreeSet, btree_map::Entry}, time::{Duration, Instant}};

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

impl Task {
    fn expire_if_needed(&mut self, now: Instant, max_attempts: u32) -> bool {
        if !matches!(&self.status, TaskStatus::Leased { until, .. } if *until <= now) {
            return false;
        }
        self.status = if self.attempts >= max_attempts {
            TaskStatus::Failed
        } else {
            TaskStatus::Queued
        };
        true
    }
}

pub struct TaskScheduler {
    tasks: BTreeMap<u64, Task>,
    // Both indexes are bounded by the admitted task count.
    queued: BTreeSet<u64>,
    leased_by_expiry: BTreeSet<(Instant, u64)>,
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
        Self::with_task_limit(tasks, max_attempts, 1_000_000)
    }

    /// Admit bounded task IDs before beginning native worker scheduling.
    pub fn with_task_limit(
        tasks: impl IntoIterator<Item = u64>, max_attempts: u32, max_tasks: usize,
    ) -> Result<Self> {
        if max_attempts == 0 {
            return Err(Error::Execution("task retry budget must be positive".into()));
        }
        if max_tasks == 0 {
            return Err(Error::Execution("task admission limit must be positive".into()));
        }
        let mut entries = BTreeMap::new();
        for id in tasks {
            let at_limit = entries.len() >= max_tasks;
            match entries.entry(id) {
                Entry::Occupied(_) => return Err(Error::Execution("duplicate task ID".into())),
                Entry::Vacant(entry) => {
                    if at_limit {
                        return Err(Error::Execution("native task admission limit exceeded".into()));
                    }
                    entry.insert(Task { attempts: 0, status: TaskStatus::Queued });
                }
            }
        }
        let queued = entries.keys().copied().collect();
        Ok(Self { tasks: entries, queued, leased_by_expiry: BTreeSet::new(), max_attempts })
    }

    pub fn lease(&mut self, worker: u64, now: Instant, ttl: Duration) -> Result<Option<Lease>> {
        if ttl.is_zero() {
            return Err(Error::Execution("task lease duration must be positive".into()));
        }
        let until = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task lease deadline overflow".into()))?;
        self.expire_leases(now);
        let Some(&id) = self.queued.iter().next() else { return Ok(None); };
        self.queued.remove(&id);
        let task = self.tasks.get_mut(&id).expect("queued task exists");
        task.attempts += 1;
        task.status = TaskStatus::Leased { worker, attempt: task.attempts, until };
        self.leased_by_expiry.insert((until, id));
        Ok(Some(Lease { task: id, worker, attempt: task.attempts }))
    }

    /// Reclaim only leases with elapsed deadlines. Runtime cost is proportional
    /// to the number of expired leases rather than all admitted tasks.
    pub fn expire_leases(&mut self, now: Instant) -> usize {
        let mut expired = 0usize;
        while let Some(&(until, id)) = self.leased_by_expiry.iter().next() {
            if until > now { break; }
            self.leased_by_expiry.remove(&(until, id));
            let task = self.tasks.get_mut(&id).expect("indexed lease exists");
            if task.expire_if_needed(now, self.max_attempts) {
                if task.status == TaskStatus::Queued { self.queued.insert(id); }
                expired += 1;
            }
        }
        expired
    }

    /// Extend an active attempt without reviving a stale or expired lease.
    pub fn renew(&mut self, lease: &Lease, now: Instant, ttl: Duration) -> Result<bool> {
        if ttl.is_zero() {
            return Err(Error::Execution("task heartbeat TTL must be positive".into()));
        }
        let deadline = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task heartbeat deadline overflow".into()))?;
        let Some(task) = self.tasks.get_mut(&lease.task) else { return Ok(false); };
        match &task.status {
            TaskStatus::Leased { worker, attempt, until }
                if *worker == lease.worker && *attempt == lease.attempt && *until > now => {
                let old_until = *until;
                let new_until = deadline.max(old_until);
                task.status = TaskStatus::Leased {
                    worker: lease.worker, attempt: lease.attempt, until: new_until
                };
                if new_until != old_until {
                    self.leased_by_expiry.remove(&(old_until, lease.task));
                    self.leased_by_expiry.insert((new_until, lease.task));
                }
                Ok(true)
            }
            _ => Ok(false),
        }
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
        self.leased_by_expiry.remove(&(until, lease.task));
        if now >= until {
            task.status = if task.attempts >= self.max_attempts {
                TaskStatus::Failed
            } else {
                TaskStatus::Queued
            };
            if task.status == TaskStatus::Queued { self.queued.insert(lease.task); }
            return false;
        }
        task.status = if success {
            TaskStatus::Done
        } else if task.attempts >= self.max_attempts {
            TaskStatus::Failed
        } else {
            TaskStatus::Queued
        };
        if task.status == TaskStatus::Queued { self.queued.insert(lease.task); }
        true
    }

    pub fn cancel_task(&mut self, id: u64) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else { return false; };
        match &task.status {
            TaskStatus::Queued => { self.queued.remove(&id); }
            TaskStatus::Leased { until, .. } => {
                self.leased_by_expiry.remove(&(*until, id));
            }
            TaskStatus::Done | TaskStatus::Failed => return false,
        }
        task.status = TaskStatus::Failed;
        true
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

    /// View a bounded, deterministic list of ready task IDs without leasing.
    pub fn queued_tasks(&self, limit: usize) -> Vec<u64> {
        self.queued.iter().take(limit).copied().collect()
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
        self.queued.clear();
        self.leased_by_expiry.clear();
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

#[cfg(test)]
mod consolidated_heartbeat_regressions {
    use super::*;
    #[test]
    fn active_worker_can_extend_but_not_resurrect_expired_lease() {
        let now = Instant::now();
        let mut queue = TaskScheduler::new([10], 2).unwrap();
        let lease = queue.lease(7, now, Duration::from_secs(2)).unwrap().unwrap();
        assert!(queue.renew(&lease, now + Duration::from_secs(1), Duration::from_secs(4)).unwrap());
        assert_eq!(queue.lease(8, now + Duration::from_secs(3), Duration::from_secs(1)).unwrap(), None);
        assert!(!queue.renew(&lease, now + Duration::from_secs(5), Duration::from_secs(1)).unwrap());
        assert!(queue.renew(&lease, now, Duration::ZERO).is_err());
    }
    #[test]
    fn heartbeat_does_not_allow_mismatched_worker_or_attempt() {
        let now = Instant::now();
        let mut queue = TaskScheduler::new([1], 3).unwrap();
        let lease = queue.lease(5, now, Duration::from_secs(2)).unwrap().unwrap();
        let mut invalid = lease.clone();
        invalid.worker = 9;
        assert!(!queue.renew(&invalid, now, Duration::from_secs(2)).unwrap());
        invalid = lease.clone();
        invalid.attempt += 1;
        assert!(!queue.renew(&invalid, now, Duration::from_secs(2)).unwrap());
    }
}

#[cfg(test)]
mod proactive_task_lease_expiry_tests {
    use super::*;

    #[test]
    fn expired_leases_are_requeued_or_failed_without_new_worker_polling() {
        let now = Instant::now();
        let ttl = Duration::from_secs(5);
        let mut queue = TaskScheduler::new([10, 20], 1).unwrap();
        let leased = queue.lease(3, now, ttl).unwrap().unwrap();
        assert_eq!(leased.task, 10);
        assert_eq!(queue.expire_leases(now), 0);
        assert_eq!(queue.expire_leases(now + ttl), 1);
        assert_eq!(queue.expire_leases(now + ttl), 0);
        assert_eq!(queue.status(10), Some(&TaskStatus::Failed));
        assert_eq!(queue.counts(), TaskCounts {
            queued: 1, leased: 0, done: 0, failed: 1,
        });

        let mut retryable = TaskScheduler::new([1], 2).unwrap();
        retryable.lease(4, now, ttl).unwrap().unwrap();
        assert_eq!(retryable.expire_leases(now + ttl), 1);
        assert_eq!(retryable.status(1), Some(&TaskStatus::Queued));
        assert_eq!(retryable.lease(5, now + ttl, ttl).unwrap().unwrap().attempt, 2);
    }
}

#[cfg(test)]
mod scheduler_ready_task_inspection_tests {
    use super::*;

    #[test]
    fn lists_ready_tasks_in_sorted_order_without_claiming_them() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([9, 1, 5, 3], 2).unwrap();
        assert_eq!(scheduler.queued_tasks(2), vec![1, 3]);
        assert_eq!(scheduler.queued_tasks(0), vec![]);
        let first = scheduler.lease(4, now, Duration::from_secs(10))
            .unwrap().unwrap();
        assert_eq!(first.task, 1);
        assert_eq!(scheduler.queued_tasks(10), vec![3, 5, 9]);
        assert_eq!(scheduler.counts().leased, 1);
        assert!(scheduler.cancel_task(3));
        assert_eq!(scheduler.queued_tasks(10), vec![5, 9]);
    }
}

#[cfg(test)]
mod native_scheduler_admission_limit_tests {
    use super::*;

    #[test]
    fn rejects_oversized_worklists_without_leasing() {
        assert!(TaskScheduler::with_task_limit([1, 2, 3], 2, 2).is_err());
        assert!(TaskScheduler::with_task_limit([], 2, 0).is_err());
        let mut queue = TaskScheduler::with_task_limit([2, 1], 2, 2).unwrap();
        assert_eq!(queue.counts().queued, 2);
        assert_eq!(queue.lease(9, Instant::now(), Duration::from_secs(5))
            .unwrap().unwrap().task, 1);
    }
}

#[cfg(test)]
mod scheduler_duplicate_admission_diagnostics_tests {
    use super::*;
    #[test]
    fn duplicate_identifiers_keep_their_error_at_the_task_limit() {
        let duplicate = TaskScheduler::with_task_limit([10, 10], 2, 1).err().unwrap();
        assert!(duplicate.to_string().contains("duplicate task ID"));
        let full = TaskScheduler::with_task_limit([10, 11], 2, 1).err().unwrap();
        assert!(full.to_string().contains("admission limit"));
        assert_eq!(TaskScheduler::with_task_limit([10], 2, 1).unwrap().counts().queued, 1);
    }
}

#[cfg(test)]
mod indexed_scheduler_regressions {
    use super::*;

    #[test]
    fn renewals_keep_expiration_index_bounded_and_ignore_old_deadlines() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([7], 3).unwrap();
        let lease = scheduler.lease(11, now, Duration::from_secs(1)).unwrap().unwrap();
        for seconds in 2..1_000 {
            assert!(scheduler.renew(&lease, now, Duration::from_secs(seconds)).unwrap());
            assert_eq!(scheduler.leased_by_expiry.len(), 1);
        }
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(500)), 0);
        assert_eq!(scheduler.status(7).unwrap(), &TaskStatus::Leased {
            worker: 11, attempt: 1, until: now + Duration::from_secs(999),
        });
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(1_000)), 1);
        assert_eq!(scheduler.queued_tasks(10), [7]);
    }

    #[test]
    fn completed_and_cancelled_tasks_leave_no_stale_lease_index_entries() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1, 2, 3], 2).unwrap();
        let first = scheduler.lease(9, now, Duration::from_secs(5)).unwrap().unwrap();
        let second = scheduler.lease(8, now, Duration::from_secs(1)).unwrap().unwrap();
        assert!(scheduler.finish_at(&first, true, now));
        assert!(scheduler.cancel_task(second.task));
        assert_eq!(scheduler.leased_by_expiry.len(), 0);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(100)), 0);
        assert_eq!(scheduler.queued_tasks(10), [3]);
        assert_eq!(scheduler.counts().done, 1);
        assert_eq!(scheduler.counts().failed, 1);
    }
}
