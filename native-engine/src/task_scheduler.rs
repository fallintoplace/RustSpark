use std::{collections::{HashMap, HashSet, BTreeSet, hash_map::Entry}, time::{Duration, Instant}};

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
    // Lookup by task ID is on the hot path of leasing, renewal and completion.
    // The separate ordered ready/deadline sets keep deterministic scheduling.
    tasks: HashMap<u64, Task>,
    // The queue and deadline index mirror the corresponding TaskStatus entries.
    ready: BTreeSet<u64>,
    deadlines: BTreeSet<(Instant, u64)>,
    // Active task IDs indexed by their current worker. Disconnect recovery
    // only touches that worker's leases, not the entire task table.
    by_worker: HashMap<u64, HashSet<u64>>,
    max_attempts: u32,
    counts: TaskCounts,
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
        let tasks = tasks.into_iter();
        let initial_capacity = tasks.size_hint().0.min(max_tasks).min(4096);
        let mut entries = HashMap::with_capacity(initial_capacity);
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
        let queued = entries.len();
        let ready = entries.keys().copied().collect();
        Ok(Self { tasks: entries, ready, deadlines: BTreeSet::new(),
            by_worker: HashMap::new(), max_attempts,
            counts: TaskCounts { queued, ..TaskCounts::default() } })
    }

    fn remove_worker_task(&mut self, worker: u64, id: u64) {
        if let Some(ids) = self.by_worker.get_mut(&worker) {
            ids.remove(&id);
            if ids.is_empty() { self.by_worker.remove(&worker); }
        }
    }

    fn lease_ready(&mut self, worker: u64, until: Instant) -> Option<Lease> {
        let id = self.ready.pop_first()?;
        let task = self.tasks.get_mut(&id).expect("queued task exists");
        task.attempts += 1;
        task.status = TaskStatus::Leased { worker, attempt: task.attempts, until };
        self.counts.queued -= 1;
        self.counts.leased += 1;
        self.deadlines.insert((until, id));
        self.by_worker.entry(worker).or_default().insert(id);
        Some(Lease { task: id, worker, attempt: task.attempts })
    }

    pub fn lease(&mut self, worker: u64, now: Instant, ttl: Duration) -> Result<Option<Lease>> {
        if ttl.is_zero() {
            return Err(Error::Execution("task lease duration must be positive".into()));
        }
        let until = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task lease deadline overflow".into()))?;
        self.expire_leases(now);
        Ok(self.lease_ready(worker, until))
    }

    /// Claim a bounded window of tasks with one expiry sweep and deadline
    /// calculation. Single-task leasing remains allocation-free.
    pub fn lease_many(
        &mut self, worker: u64, now: Instant, ttl: Duration, max_tasks: usize,
    ) -> Result<Vec<Lease>> {
        if ttl.is_zero() {
            return Err(Error::Execution("task lease duration must be positive".into()));
        }
        let until = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task lease deadline overflow".into()))?;
        self.expire_leases(now);
        let count = max_tasks.min(self.ready.len());
        let mut leases = Vec::with_capacity(count);
        for _ in 0..count {
            leases.push(self.lease_ready(worker, until).expect("ready task exists"));
        }
        Ok(leases)
    }

    /// Reclaim expired leases independently of worker requests, so monitoring
    /// and retry orchestration can observe current task states.
    pub fn expire_leases(&mut self, now: Instant) -> usize {
        let mut expired = 0usize;
        while let Some((until, id)) = self.deadlines.first().copied() {
            if until > now { break; }
            self.deadlines.pop_first();
            let task = self.tasks.get_mut(&id).expect("deadline task exists");
            let worker = match &task.status {
                TaskStatus::Leased { worker, .. } => Some(*worker),
                _ => None,
            };
            if task.expire_if_needed(now, self.max_attempts) {
                self.counts.leased -= 1;
                if task.status == TaskStatus::Queued {
                    self.counts.queued += 1;
                    self.ready.insert(id);
                } else {
                    self.counts.failed += 1;
                }
                if let Some(worker) = worker { self.remove_worker_task(worker, id); }
                expired += 1;
            }
        }
        expired
    }

    fn renew_with_deadline(&mut self, lease: &Lease, now: Instant, deadline: Instant) -> bool {
        let Some(task) = self.tasks.get_mut(&lease.task) else { return false; };
        match &task.status {
            TaskStatus::Leased { worker, attempt, until }
                if *worker == lease.worker && *attempt == lease.attempt && *until > now => {
                let old_until = *until;
                let new_until = deadline.max(old_until);
                task.status = TaskStatus::Leased {
                    worker: lease.worker, attempt: lease.attempt, until: new_until,
                };
                if new_until != old_until {
                    self.deadlines.remove(&(old_until, lease.task));
                    self.deadlines.insert((new_until, lease.task));
                }
                true
            }
            _ => false,
        }
    }

    /// Extend an active attempt without reviving a stale or expired lease.
    pub fn renew(&mut self, lease: &Lease, now: Instant, ttl: Duration) -> Result<bool> {
        if ttl.is_zero() {
            return Err(Error::Execution("task heartbeat TTL must be positive".into()));
        }
        let deadline = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task heartbeat deadline overflow".into()))?;
        Ok(self.renew_with_deadline(lease, now, deadline))
    }

    /// Renew a worker's lease window with one clock and deadline calculation.
    /// Results correspond to input order; an invalid attempt cannot extend
    /// another worker's lease. A zero TTL rejects the whole batch up front.
    pub fn renew_many(
        &mut self, leases: &[Lease], now: Instant, ttl: Duration,
    ) -> Result<Vec<bool>> {
        if ttl.is_zero() {
            return Err(Error::Execution("task heartbeat TTL must be positive".into()));
        }
        let deadline = now.checked_add(ttl)
            .ok_or_else(|| Error::Execution("task heartbeat deadline overflow".into()))?;
        Ok(leases.iter().map(|lease| {
            self.renew_with_deadline(lease, now, deadline)
        }).collect())
    }

    pub fn finish(&mut self, lease: &Lease, success: bool) -> bool {
        self.finish_at(lease, success, Instant::now())
    }

    pub fn finish_at(&mut self, lease: &Lease, success: bool, now: Instant) -> bool {
        let until = match self.tasks.get(&lease.task).map(|task| &task.status) {
            Some(TaskStatus::Leased { worker, attempt, until })
                if *worker == lease.worker && *attempt == lease.attempt => *until,
            _ => return false,
        };
        self.deadlines.remove(&(until, lease.task));
        self.remove_worker_task(lease.worker, lease.task);
        let task = self.tasks.get_mut(&lease.task).expect("validated active task");
        self.counts.leased -= 1;
        if now >= until {
            task.status = if task.attempts >= self.max_attempts {
                TaskStatus::Failed
            } else {
                TaskStatus::Queued
            };
            if task.status == TaskStatus::Queued {
                self.counts.queued += 1;
                self.ready.insert(lease.task);
            } else { self.counts.failed += 1; }
            return false;
        }
        task.status = if success {
            TaskStatus::Done
        } else if task.attempts >= self.max_attempts {
            TaskStatus::Failed
        } else {
            TaskStatus::Queued
        };
        match task.status {
            TaskStatus::Done => self.counts.done += 1,
            TaskStatus::Failed => self.counts.failed += 1,
            TaskStatus::Queued => {
                self.counts.queued += 1;
                self.ready.insert(lease.task);
            }
            TaskStatus::Leased { .. } => unreachable!("finished lease is no longer active"),
        }
        true
    }

    /// Reclaim one disconnected worker's leases without scanning unrelated
    /// tasks. Retry attempts stay consumed and stale completions are rejected.
    pub fn reclaim_worker(&mut self, worker: u64) -> usize {
        let Some(leased) = self.by_worker.remove(&worker) else { return 0; };
        for &id in &leased {
            let task = self.tasks.get_mut(&id).expect("leased task exists");
            let until = match &task.status {
                TaskStatus::Leased { worker: owner, until, .. } if *owner == worker => *until,
                _ => unreachable!("worker index mirrors active task leases"),
            };
            self.deadlines.remove(&(until, id));
            self.counts.leased -= 1;
            if task.attempts >= self.max_attempts {
                task.status = TaskStatus::Failed;
                self.counts.failed += 1;
            } else {
                task.status = TaskStatus::Queued;
                self.counts.queued += 1;
                self.ready.insert(id);
            }
        }
        leased.len()
    }

    pub fn cancel_task(&mut self, id: u64) -> bool {
        let Some(task) = self.tasks.get_mut(&id) else { return false; };
        if matches!(task.status, TaskStatus::Queued | TaskStatus::Leased { .. }) {
            if task.status == TaskStatus::Queued { self.counts.queued -= 1; }
            else { self.counts.leased -= 1; }
            self.counts.failed += 1;
            self.ready.remove(&id);
            let worker = if let TaskStatus::Leased { worker, until, .. } = &task.status {
                self.deadlines.remove(&(*until, id));
                Some(*worker)
            } else { None };
            task.status = TaskStatus::Failed;
            if let Some(worker) = worker { self.remove_worker_task(worker, id); }
            return true;
        }
        false
    }

    /// Read scheduler state in O(1), even for a million tasks.
    pub fn counts(&self) -> TaskCounts { self.counts }

    /// View a bounded, deterministic list of ready task IDs without leasing.
    pub fn queued_tasks(&self, limit: usize) -> Vec<u64> {
        self.ready.iter().copied().take(limit).collect()
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
        self.counts.failed += cancelled;
        self.counts.queued = 0;
        self.counts.leased = 0;
        self.ready.clear();
        self.deadlines.clear();
        self.by_worker.clear();
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
        assert_eq!(scheduler.queued_tasks(0), Vec::<u64>::new());
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
        assert!(TaskScheduler::with_task_limit(std::iter::empty::<u64>(), 2, 0).is_err());
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
    fn renewal_removes_old_deadline_and_expiry_requeues_in_id_order() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([4, 2, 1, 3], 3).unwrap();
        let first = scheduler.lease(7, now, Duration::from_secs(2)).unwrap().unwrap();
        let second = scheduler.lease(8, now, Duration::from_secs(2)).unwrap().unwrap();
        assert_eq!((first.task, second.task), (1, 2));
        assert!(scheduler.renew(&second, now + Duration::from_secs(1),
            Duration::from_secs(10)).unwrap());
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(3)), 1);
        assert_eq!(scheduler.queued_tasks(10), vec![1, 3, 4]);
        let retry = scheduler.lease(9, now + Duration::from_secs(3),
            Duration::from_secs(2)).unwrap().unwrap();
        assert_eq!(retry.task, first.task);
        assert_ne!(retry.attempt, first.attempt);
        assert!(!scheduler.finish_at(&first, true, now + Duration::from_secs(3)));
        assert!(scheduler.cancel_task(second.task));
        assert!(!scheduler.renew(&second, now + Duration::from_secs(4),
            Duration::from_secs(10)).unwrap());
        assert!(scheduler.finish_at(&retry, true, now + Duration::from_secs(4)));
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(20)), 0);
        assert_eq!(scheduler.queued_tasks(10), vec![3, 4]);
        assert_eq!(scheduler.counts(), TaskCounts { queued: 2, leased: 0,
            done: 1, failed: 1 });
    }

    #[test]
    fn large_task_queue_leases_in_sorted_order_without_scanning_each_task() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new((0..3_000).rev(), 1).unwrap();
        for id in 0..3_000 {
            let lease = scheduler.lease(1, now, Duration::from_secs(1))
                .unwrap().unwrap();
            assert_eq!(lease.task, id);
        }
        assert!(scheduler.queued_tasks(1).is_empty());
        assert_eq!(scheduler.counts().leased, 3_000);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(1)), 3_000);
        assert_eq!(scheduler.counts().failed, 3_000);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(2)), 0);
        assert_eq!(scheduler.lease(1, now + Duration::from_secs(2),
            Duration::from_secs(1)).unwrap(), None);
    }
}

#[cfg(test)]
mod constant_time_task_counts_regressions {
    use super::*;

    fn recompute(scheduler: &TaskScheduler) -> TaskCounts {
        let mut counts = TaskCounts::default();
        for task in scheduler.tasks.values() {
            match task.status {
                TaskStatus::Queued => counts.queued += 1,
                TaskStatus::Leased { .. } => counts.leased += 1,
                TaskStatus::Done => counts.done += 1,
                TaskStatus::Failed => counts.failed += 1,
            }
        }
        counts
    }

    #[test]
    fn cached_counts_match_statuses_through_retries_expiry_and_cancellation() {
        let now = Instant::now();
        let ttl = Duration::from_secs(3);
        let mut scheduler = TaskScheduler::new(0..200, 2).unwrap();
        assert_eq!(scheduler.counts(), recompute(&scheduler));
        let leases = (0..60).map(|worker| {
            let lease = scheduler.lease(worker, now, ttl).unwrap().unwrap();
            assert_eq!(scheduler.counts(), recompute(&scheduler));
            lease
        }).collect::<Vec<_>>();
        for lease in &leases[..20] {
            assert!(scheduler.finish_at(lease, true, now));
            assert_eq!(scheduler.counts(), recompute(&scheduler));
        }
        for lease in &leases[20..40] {
            assert!(scheduler.finish_at(lease, false, now));
            assert_eq!(scheduler.counts(), recompute(&scheduler));
        }
        assert_eq!(scheduler.expire_leases(now + ttl), 20);
        assert_eq!(scheduler.counts(), recompute(&scheduler));
        for _ in 0..30 {
            let lease = scheduler.lease(99, now + ttl, ttl).unwrap().unwrap();
            assert!(scheduler.cancel_task(lease.task));
            assert_eq!(scheduler.counts(), recompute(&scheduler));
        }
        scheduler.cancel_pending();
        assert_eq!(scheduler.counts(), recompute(&scheduler));
        assert_eq!(scheduler.counts().queued + scheduler.counts().leased, 0);
    }
}

#[cfg(test)]
mod batched_native_scheduler_lease_tests {
    use super::*;

    #[test]
    fn claims_worker_windows_without_duplicate_or_missing_tasks() {
        let now = Instant::now();
        let ttl = Duration::from_secs(5);
        let mut scheduler = TaskScheduler::new([4, 1, 3, 2], 3).unwrap();
        let first = scheduler.lease_many(7, now, ttl, 3).unwrap();
        assert_eq!(first.iter().map(|lease| lease.task).collect::<Vec<_>>(), [1, 2, 3]);
        assert!(first.iter().all(|lease| lease.worker == 7 && lease.attempt == 1));
        assert_eq!(scheduler.counts(), TaskCounts {
            queued: 1, leased: 3, done: 0, failed: 0,
        });
        let second = scheduler.lease_many(8, now, ttl, 100).unwrap();
        assert_eq!(second.iter().map(|lease| lease.task).collect::<Vec<_>>(), [4]);
        assert!(scheduler.lease_many(9, now, ttl, 100).unwrap().is_empty());
        assert!(scheduler.lease_many(9, now, ttl, 0).unwrap().is_empty());
        assert!(scheduler.lease_many(9, now, Duration::ZERO, 2).is_err());

        assert!(scheduler.renew(&first[1], now + Duration::from_secs(2),
            Duration::from_secs(10)).unwrap());
        let retries = scheduler.lease_many(10, now + Duration::from_secs(6), ttl, 10).unwrap();
        assert_eq!(retries.iter().map(|lease| lease.task).collect::<Vec<_>>(),
            [1, 3, 4]);
        assert!(retries.iter().all(|lease| lease.attempt == 2));
        assert!(!scheduler.finish_at(&first[0], true, now + Duration::from_secs(6)));
        assert_eq!(scheduler.counts(), TaskCounts {
            queued: 0, leased: 4, done: 0, failed: 0,
        });
    }
}

#[cfg(test)]
mod hash_index_scheduler_regressions {
    use super::*;

    #[test]
    fn unordered_admission_keeps_sorted_claims_and_correct_lease_expiry() {
        let ids = (0u64..10_000).rev().map(|id| id * 17);
        let mut scheduler = TaskScheduler::new(ids, 2).unwrap();
        let now = Instant::now();
        let leased = scheduler.lease_many(7, now, Duration::from_secs(10), 512).unwrap();
        assert_eq!(leased.len(), 512);
        assert_eq!(leased[0].task, 0);
        assert_eq!(leased[1].task, 17);
        assert_eq!(leased[511].task, 511 * 17);
        for lease in leased.iter().take(256) {
            assert!(scheduler.finish_at(lease, true, now + Duration::from_secs(1)));
        }
        assert_eq!(scheduler.counts().done, 256);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(10)), 256);
        assert_eq!(scheduler.counts().queued, 10_000 - 512 + 256);
        assert_eq!(scheduler.status(0), Some(&TaskStatus::Done));
        assert_eq!(scheduler.status(256 * 17), Some(&TaskStatus::Queued));
        assert!(scheduler.cancel_task(256 * 17));
        assert_eq!(scheduler.status(256 * 17), Some(&TaskStatus::Failed));
    }

    #[test]
    fn duplicates_and_budget_rejection_remain_exact() {
        assert!(TaskScheduler::with_task_limit([5, 1, 5], 2, 3).is_err());
        assert!(TaskScheduler::with_task_limit([5, 1, 2], 2, 2).is_err());
        let scheduler = TaskScheduler::with_task_limit([5, 1, 2], 2, 3).unwrap();
        assert_eq!(scheduler.queued_tasks(10), vec![1, 2, 5]);
    }
}

#[cfg(test)]
mod batched_heartbeat_regressions {
    use super::*;

    #[test]
    fn renews_valid_leases_without_reviving_stale_or_foreign_attempts() {
        let now = Instant::now();
        let ttl = Duration::from_secs(5);
        let mut scheduler = TaskScheduler::new([10, 20, 30], 3).unwrap();
        let leases = scheduler.lease_many(7, now, ttl, 3).unwrap();
        let mut foreign = leases[1].clone();
        foreign.worker = 99;
        assert!(scheduler.renew_many(&leases, now, Duration::ZERO).is_err());
        assert_eq!(scheduler.renew_many(
            &[leases[0].clone(), foreign, leases[2].clone()],
            now + Duration::from_secs(3), Duration::from_secs(10),
        ).unwrap(), [true, false, true]);
        assert_eq!(scheduler.counts().leased, 3);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(6)), 1);
        assert_eq!(scheduler.status(20), Some(&TaskStatus::Queued));
        assert_eq!(scheduler.counts(), TaskCounts {
            queued: 1, leased: 2, done: 0, failed: 0,
        });
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(13)), 2);
        assert!(scheduler.renew_many(
            &[leases[0].clone()], now + Duration::from_secs(14), ttl,
        ).unwrap().iter().all(|accepted| !accepted));
    }

    #[test]
    fn a_new_deadline_never_shortens_an_existing_lease() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1], 2).unwrap();
        let lease = scheduler.lease(9, now, Duration::from_secs(30))
            .unwrap().unwrap();
        assert_eq!(scheduler.renew_many(
            &[lease.clone()], now, Duration::from_secs(1),
        ).unwrap(), [true]);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(2)), 0);
        assert!(scheduler.finish_at(&lease, true, now + Duration::from_secs(3)));
    }
}

#[cfg(test)]
mod worker_disconnect_recovery_tests {
    use super::*;

    #[test]
    fn disconnected_worker_requeues_only_its_own_attempts() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([1, 2, 3], 2).unwrap();
        let abandoned = scheduler.lease_many(11, now, Duration::from_secs(60), 2).unwrap();
        let alive = scheduler.lease(12, now, Duration::from_secs(60))
            .unwrap().unwrap();
        assert_eq!(scheduler.reclaim_worker(11), 2);
        assert_eq!(scheduler.reclaim_worker(11), 0);
        assert_eq!(scheduler.counts(), TaskCounts {
            queued: 2, leased: 1, done: 0, failed: 0,
        });
        for lease in &abandoned {
            assert!(!scheduler.finish_at(lease, true, now));
        }
        assert_eq!(scheduler.status(alive.task), Some(&TaskStatus::Leased {
            worker: 12, attempt: 1, until: now + Duration::from_secs(60),
        }));
        let retried = scheduler.lease_many(13, now, Duration::from_secs(60), 2).unwrap();
        assert_eq!(retried.iter().map(|lease| lease.task).collect::<Vec<_>>(),
            abandoned.iter().map(|lease| lease.task).collect::<Vec<_>>());
        assert!(retried.iter().all(|lease| lease.attempt == 2));
        assert_eq!(scheduler.reclaim_worker(13), 2);
        assert_eq!(scheduler.counts(), TaskCounts {
            queued: 0, leased: 1, done: 0, failed: 2,
        });
        assert!(retried.iter().all(|lease| scheduler.status(lease.task) == Some(&TaskStatus::Failed)));
        assert!(scheduler.finish_at(&alive, true, now));
        assert_eq!(scheduler.counts().done, 1);
    }

    #[test]
    fn worker_recovery_removes_deadlines_and_prevents_late_commits() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new([42], 3).unwrap();
        let original = scheduler.lease(3, now, Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(scheduler.reclaim_worker(3), 1);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(2)), 0);
        let retry = scheduler.lease(4, now + Duration::from_secs(2),
            Duration::from_secs(10)).unwrap().unwrap();
        assert_eq!(retry.attempt, 2);
        assert!(!scheduler.finish_at(&original, true, now));
        assert!(scheduler.finish_at(&retry, true, now + Duration::from_secs(2)));
    }
}

#[cfg(test)]
mod worker_reclaim_index_regressions {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn assert_active_index_matches_statuses(scheduler: &TaskScheduler) {
        let mut expected = HashMap::<u64, HashSet<u64>>::new();
        for (&id, task) in &scheduler.tasks {
            if let TaskStatus::Leased { worker, .. } = &task.status {
                expected.entry(*worker).or_default().insert(id);
            }
        }
        assert_eq!(scheduler.by_worker, expected);
    }

    #[test]
    fn disconnected_worker_only_reclaims_its_live_attempts() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new(0..4_096, 3).unwrap();
        let a = scheduler.lease_many(10, now, Duration::from_secs(5), 1_000).unwrap();
        let b = scheduler.lease_many(20, now, Duration::from_secs(12), 1_000).unwrap();
        assert_active_index_matches_statuses(&scheduler);
        assert!(scheduler.finish_at(&a[0], true, now));
        assert!(scheduler.cancel_task(a[1].task));
        assert!(scheduler.renew(&b[0], now, Duration::from_secs(30)).unwrap());
        assert_eq!(scheduler.reclaim_worker(10), 998);
        assert_eq!(scheduler.reclaim_worker(10), 0);
        assert!(!scheduler.finish_at(&a[2], true, now));
        assert_active_index_matches_statuses(&scheduler);
        assert_eq!(scheduler.counts().leased, 1_000);
        assert!(scheduler.finish_at(&b[0], true, now));
        assert_active_index_matches_statuses(&scheduler);
        assert_eq!(scheduler.reclaim_worker(20), 999);
        assert_active_index_matches_statuses(&scheduler);
    }

    #[test]
    fn lease_expiry_retries_and_cancellation_keep_index_consistent() {
        let now = Instant::now();
        let mut scheduler = TaskScheduler::new(0..16, 2).unwrap();
        let first = scheduler.lease_many(7, now, Duration::from_secs(2), 8).unwrap();
        let second = scheduler.lease_many(8, now, Duration::from_secs(8), 8).unwrap();
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(3)), 8);
        assert_active_index_matches_statuses(&scheduler);
        assert!(!scheduler.finish_at(&first[0], true, now + Duration::from_secs(3)));
        let retried = scheduler.lease_many(
            9, now + Duration::from_secs(3), Duration::from_secs(2), 8,
        ).unwrap();
        assert_eq!(retried.len(), 8);
        assert_active_index_matches_statuses(&scheduler);
        assert_eq!(scheduler.expire_leases(now + Duration::from_secs(6)), 8);
        assert_active_index_matches_statuses(&scheduler);
        assert_eq!(scheduler.reclaim_worker(8), 8);
        assert_active_index_matches_statuses(&scheduler);
        assert!(scheduler.cancel_pending() > 0);
        assert_active_index_matches_statuses(&scheduler);
        assert!(!scheduler.finish_at(&second[0], true, now + Duration::from_secs(7)));
    }
}
