use std::collections::{HashMap, HashSet, VecDeque};

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stage {
    pub id: u64,
    pub dependencies: Vec<u64>,
    pub partitions: usize,
}

#[derive(Clone, Debug)]
pub struct StageGraph {
    stages: Vec<Stage>,
    order: Vec<u64>,
    order_indices: Vec<usize>,
    critical_order_indices: Vec<usize>,
    longest_chain: usize,
}

impl StageGraph {
    fn dependency_ready(stage: &Stage, completed: &HashSet<u64>) -> bool {
        !completed.contains(&stage.id)
            && stage.dependencies.iter().all(|dependency| completed.contains(dependency))
    }

    pub fn new(stages: Vec<Stage>) -> Result<Self> {
        if stages.len() > 4096 {
            return Err(Error::Execution("execution graph exceeds 4096 stages".into()));
        }
        let mut index = HashMap::new();
        let mut total_partitions = 0usize;
        for (position, stage) in stages.iter().enumerate() {
            if stage.partitions == 0 || stage.partitions > 65_536 {
                return Err(Error::Execution("stage partition count must be in 1..=65536".into()));
            }
            total_partitions = total_partitions.checked_add(stage.partitions)
                .filter(|total| *total <= 1_000_000)
                .ok_or_else(|| Error::Execution("execution graph partition budget exceeded".into()))?;
            if index.insert(stage.id, position).is_some() {
                return Err(Error::Execution("duplicate stage ID".into()));
            }
        }
        let mut indegree = vec![0usize; stages.len()];
        let mut successors = vec![Vec::new(); stages.len()];
        for (position, stage) in stages.iter().enumerate() {
            // Most distributed plans have unary stages or binary joins.
            // Avoid a HashSet allocation for those common fan-ins.
            if stage.dependencies.len() == 2 &&
                stage.dependencies[0] == stage.dependencies[1] {
                return Err(Error::Execution("duplicate stage dependency".into()));
            }
            let mut seen = (stage.dependencies.len() > 2).then(HashSet::new);
            for dep in &stage.dependencies {
                if seen.as_mut().is_some_and(|keys| !keys.insert(*dep)) {
                    return Err(Error::Execution("duplicate stage dependency".into()));
                }
                let source = *index.get(dep)
                    .ok_or_else(|| Error::Execution("stage refers to unknown dependency".into()))?;
                indegree[position] += 1;
                successors[source].push(position);
            }
        }
        let mut ready = VecDeque::new();
        for (index, degree) in indegree.iter().enumerate() {
            if *degree == 0 {
                ready.push_back(index);
            }
        }
        let mut order = Vec::with_capacity(stages.len());
        let mut order_indices = Vec::with_capacity(stages.len());
        let mut depth = vec![1usize; stages.len()];
        while let Some(position) = ready.pop_front() {
            order.push(stages[position].id);
            order_indices.push(position);
            for &successor in &successors[position] {
                depth[successor] = depth[successor].max(depth[position] + 1);
                indegree[successor] -= 1;
                if indegree[successor] == 0 {
                    ready.push_back(successor);
                }
            }
        }
        if order.len() != stages.len() {
            return Err(Error::Execution("execution stage graph contains a cycle".into()));
        }
        let longest_chain = depth.into_iter().max().unwrap_or(0);
        // Compute downstream chain lengths once while successor links are available.
        // Scheduling repeatedly reads these priorities without rebuilding the graph.
        let mut depth_to_leaf = vec![1usize; stages.len()];
        for &position in order_indices.iter().rev() {
            for &child in &successors[position] {
                depth_to_leaf[position] = depth_to_leaf[position]
                    .max(depth_to_leaf[child] + 1);
            }
        }
        // Critical-path priority is fixed for the lifetime of a stage graph.
        // Sort once instead of allocating and sorting runnable IDs per poll.
        let mut critical_order_indices = order_indices.clone();
        critical_order_indices.sort_unstable_by_key(|&position| {
            (std::cmp::Reverse(depth_to_leaf[position]), stages[position].id)
        });
        Ok(Self { stages, order, order_indices, critical_order_indices, longest_chain })
    }

    pub fn topological_order(&self) -> &[u64] {
        &self.order
    }

    pub fn ready(&self, completed: &HashSet<u64>) -> Vec<u64> {
        self.ready_excluding(completed, &HashSet::new())
    }

    /// Runnable stages that have not already been dispatched.
    pub fn ready_excluding(
        &self, completed: &HashSet<u64>, running: &HashSet<u64>
    ) -> Vec<u64> {
        self.order_indices.iter().filter_map(|&index| {
            let stage = &self.stages[index];
            (Self::dependency_ready(stage, completed) && !running.contains(&stage.id))
                .then_some(stage.id)
        }).collect()
    }

    /// Dependency-ready stages ordered by longest remaining downstream chain.
    pub fn ready_by_critical_path(&self, completed: &HashSet<u64>) -> Vec<u64> {
        self.critical_order_indices.iter().filter_map(|&position| {
            let stage = &self.stages[position];
            Self::dependency_ready(stage, completed).then_some(stage.id)
        }).collect()
    }

    /// Return runnable stages in topology order without exceeding worker capacity.
    /// A stage is scheduled only when all of its partitions fit the budget.
    pub fn ready_with_partition_budget(
        &self, completed: &HashSet<u64>, max_partitions: usize,
    ) -> Vec<u64> {
        let mut remaining = max_partitions;
        self.order_indices.iter().filter_map(|&index| {
            let stage = &self.stages[index];
            if !Self::dependency_ready(stage, completed)
                || stage.partitions > remaining
            {
                return None;
            }
            remaining -= stage.partitions;
            Some(stage.id)
        }).collect()
    }

    /// Enumerate runnable partition tasks in topological order without exceeding
    /// the caller's scheduling window. Completed stages are not rescheduled.
    pub fn ready_tasks(&self, completed: &HashSet<u64>, max_tasks: usize) -> Vec<(u64, usize)> {
        self.ready_tasks_excluding(completed, &HashSet::new(), max_tasks)
    }

    /// Issue a new scheduling window without resending partitions already
    /// dispatched to workers. A stage stays blocked until all upstream stages
    /// appear in completed, including tasks still running upstream.
    pub fn ready_tasks_excluding(
        &self, completed: &HashSet<u64>,
        running: &HashSet<(u64, usize)>, max_tasks: usize,
    ) -> Vec<(u64, usize)> {
        self.ready_tasks_in_order(&self.order_indices, completed, running, max_tasks)
    }

    /// Claim partition tasks from stages with the longest downstream path
    /// first. Dependents remain blocked until upstream stages complete.
    pub fn ready_tasks_by_critical_path(
        &self, completed: &HashSet<u64>, max_tasks: usize,
    ) -> Vec<(u64, usize)> {
        self.ready_tasks_by_critical_path_excluding(completed, &HashSet::new(), max_tasks)
    }

    /// Critical-path worker window that excludes partitions already claimed.
    pub fn ready_tasks_by_critical_path_excluding(
        &self, completed: &HashSet<u64>,
        running: &HashSet<(u64, usize)>, max_tasks: usize,
    ) -> Vec<(u64, usize)> {
        self.ready_tasks_in_order(&self.critical_order_indices, completed, running, max_tasks)
    }

    fn ready_tasks_in_order(
        &self, order_indices: &[usize], completed: &HashSet<u64>,
        running: &HashSet<(u64, usize)>, max_tasks: usize,
    ) -> Vec<(u64, usize)> {
        let mut tasks = Vec::with_capacity(max_tasks.min(1024));
        if max_tasks == 0 { return tasks; }
        if running.is_empty() {
            // Initial dispatch has no exclusions. Fill windows by range,
            // avoiding one hash-table probe per partition.
            for &index in order_indices {
                let stage = &self.stages[index];
                if !Self::dependency_ready(stage, completed) { continue; }
                let take = stage.partitions.min(max_tasks - tasks.len());
                tasks.extend((0..take).map(|partition| (stage.id, partition)));
                if tasks.len() == max_tasks { return tasks; }
            }
            return tasks;
        }
        if running.len() >= 512 {
            // A worker window with most partitions already dispatched should
            // not scan and hash-probe every occupied partition on every poll.
            // Sort the exclusions once and issue whole free partition ranges.
            let mut excluded: HashMap<u64, Vec<usize>> = HashMap::new();
            for &(stage, partition) in running {
                excluded.entry(stage).or_default().push(partition);
            }
            for partitions in excluded.values_mut() { partitions.sort_unstable(); }
            for &index in order_indices {
                let stage = &self.stages[index];
                if !Self::dependency_ready(stage, completed) { continue; }
                let mut next = 0usize;
                if let Some(partitions) = excluded.get(&stage.id) {
                    for &blocked in partitions {
                        if blocked >= stage.partitions { break; }
                        if blocked > next {
                            let take = (blocked - next).min(max_tasks - tasks.len());
                            tasks.extend((next..next + take).map(|p| (stage.id, p)));
                            if tasks.len() == max_tasks { return tasks; }
                        }
                        next = blocked + 1;
                    }
                }
                let take = (stage.partitions - next).min(max_tasks - tasks.len());
                tasks.extend((next..next + take).map(|p| (stage.id, p)));
                if tasks.len() == max_tasks { return tasks; }
            }
            return tasks;
        }
        for &index in order_indices {
            let stage = &self.stages[index];
            if !Self::dependency_ready(stage, completed) {
                continue;
            }
            for partition in 0..stage.partitions {
                let task = (stage.id, partition);
                if running.contains(&task) { continue; }
                tasks.push(task);
                if tasks.len() == max_tasks { return tasks; }
            }
        }
        tasks
    }

    pub fn stages(&self) -> &[Stage] {
        &self.stages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_order_and_ready_stages() {
        let graph = StageGraph::new(vec![
            Stage { id: 3, dependencies: vec![1, 2], partitions: 8 },
            Stage { id: 1, dependencies: vec![], partitions: 4 },
            Stage { id: 2, dependencies: vec![], partitions: 4 },
        ]).unwrap();
        assert_eq!(graph.topological_order(), [1, 2, 3]);
        assert_eq!(graph.ready(&HashSet::from([1, 2])), [3]);
    }

    #[test]
    fn partition_fanout_has_a_bounded_budget() {
        assert!(StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![], partitions: 65_537 },
        ]).is_err());
        let stages = (0..16).map(|id| Stage {
            id, dependencies: vec![], partitions: 65_536,
        }).collect::<Vec<_>>();
        assert!(StageGraph::new(stages).is_err());
    }

    #[test]
    fn rejects_cycles_and_missing_stages() {
        assert!(StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![2], partitions: 1 },
            Stage { id: 2, dependencies: vec![1], partitions: 1 },
        ]).is_err());
        assert!(StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![99], partitions: 1 },
        ]).is_err());
    }
}

impl StageGraph {
    /// Number of stages in the longest dependency chain (zero for an empty graph).
    pub fn longest_dependency_chain(&self) -> usize {
        self.longest_chain
    }
}

#[cfg(test)]
mod stage_chain_feature_tests {
    use super::*;

    #[test]
    fn chain_length_counts_dependencies_not_parallel_siblings() {
        let graph = StageGraph::new(vec![
            Stage { id: 4, dependencies: vec![2, 3], partitions: 8 },
            Stage { id: 3, dependencies: vec![1], partitions: 2 },
            Stage { id: 2, dependencies: vec![], partitions: 2 },
            Stage { id: 1, dependencies: vec![], partitions: 2 },
        ]).unwrap();
        assert_eq!(graph.longest_dependency_chain(), 3);
        assert_eq!(StageGraph::new(vec![]).unwrap().longest_dependency_chain(), 0);
    }
}

#[cfg(test)]
mod ready_topology_tests {
    use super::*;

    #[test]
    fn runnable_stages_follow_topological_order() {
        let graph = StageGraph::new(vec![
            Stage { id: 8, dependencies: vec![1], partitions: 1 },
            Stage { id: 9, dependencies: vec![], partitions: 1 },
            Stage { id: 1, dependencies: vec![], partitions: 1 },
        ]).unwrap();
        let available = graph.ready(&HashSet::new());
        assert_eq!(available, graph.topological_order().iter().copied()
            .filter(|id| *id != 8).collect::<Vec<_>>());
        assert_eq!(graph.ready(&HashSet::from([1, 9])), [8]);
    }
}

#[cfg(test)]
mod partition_budget_ready_tests {
    use super::*;

    #[test]
    fn excludes_incomplete_and_oversized_stages_without_starving_smaller_ones() {
        let graph = StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![], partitions: 4 },
            Stage { id: 2, dependencies: vec![], partitions: 7 },
            Stage { id: 3, dependencies: vec![], partitions: 2 },
            Stage { id: 4, dependencies: vec![1], partitions: 1 },
        ]).unwrap();
        assert_eq!(graph.ready_with_partition_budget(&HashSet::new(), 6), [1, 3]);
        assert_eq!(graph.ready_with_partition_budget(&HashSet::from([1]), 2), [3]);
        assert_eq!(graph.ready_with_partition_budget(&HashSet::from([1, 3]), 1), [4]);
        assert!(graph.ready_with_partition_budget(&HashSet::new(), 0).is_empty());
    }
}

#[cfg(test)]
mod native_critical_path_ready_tests {
    use super::*;

    #[test]
    fn favors_longest_unblocked_path_with_deterministic_ties() {
        let graph = StageGraph::new(vec![
            Stage { id: 50, dependencies: vec![40], partitions: 1 },
            Stage { id: 20, dependencies: vec![], partitions: 1 },
            Stage { id: 40, dependencies: vec![30], partitions: 1 },
            Stage { id: 30, dependencies: vec![10], partitions: 1 },
            Stage { id: 10, dependencies: vec![], partitions: 1 },
            Stage { id: 21, dependencies: vec![], partitions: 1 },
        ]).unwrap();
        assert_eq!(graph.ready_by_critical_path(&HashSet::new()), [10, 20, 21]);
        assert_eq!(graph.ready_by_critical_path(&HashSet::from([10])),
            [30, 20, 21]);
        assert_eq!(graph.ready_by_critical_path(&HashSet::from([10, 30, 40])),
            [20, 21, 50]);
        assert!(StageGraph::new(vec![]).unwrap()
            .ready_by_critical_path(&HashSet::new()).is_empty());
    }
}

#[cfg(test)]
mod cached_stage_chain_depth_tests {
    use super::*;
    #[test]
    fn longest_path_tracks_diamond_dependencies() {
        let graph = StageGraph::new(vec![
            Stage { id: 10, dependencies: vec![8, 9], partitions: 1 },
            Stage { id: 8, dependencies: vec![4], partitions: 1 },
            Stage { id: 9, dependencies: vec![4], partitions: 1 },
            Stage { id: 4, dependencies: vec![], partitions: 1 },
            Stage { id: 20, dependencies: vec![], partitions: 1 },
        ]).unwrap();
        assert_eq!(graph.longest_dependency_chain(), 3);
        assert_eq!(graph.longest_dependency_chain(), 3);
        assert_eq!(StageGraph::new(vec![]).unwrap().longest_dependency_chain(), 0);
    }
}

#[cfg(test)]
mod inflight_stage_dispatch_tests {
    use super::*;

    #[test]
    fn running_tasks_are_not_dispatched_again() {
        let graph = StageGraph::new(vec![
            Stage { id: 30, dependencies: vec![10, 20], partitions: 3 },
            Stage { id: 10, dependencies: vec![], partitions: 1 },
            Stage { id: 20, dependencies: vec![], partitions: 2 },
        ]).unwrap();
        assert_eq!(graph.ready_excluding(&HashSet::new(), &HashSet::from([10])), [20]);
        assert!(graph.ready_excluding(&HashSet::new(), &HashSet::from([10, 20])).is_empty());
        assert_eq!(graph.ready_excluding(&HashSet::from([10, 20]), &HashSet::new()), [30]);
        assert!(graph.ready_excluding(&HashSet::from([10, 20]), &HashSet::from([30])).is_empty());
    }
}

#[cfg(test)]
mod bounded_stage_partition_tasks_tests {
    use super::*;

    #[test]
    fn enumerates_ready_partition_tasks_in_topological_order() {
        let graph = StageGraph::new(vec![
            Stage { id: 9, dependencies: vec![2], partitions: 3 },
            Stage { id: 2, dependencies: vec![], partitions: 2 },
            Stage { id: 4, dependencies: vec![], partitions: 1 },
        ]).unwrap();
        assert_eq!(graph.ready_tasks(&HashSet::new(), 3), vec![(2, 0), (2, 1), (4, 0)]);
        assert_eq!(graph.ready_tasks(&HashSet::new(), 0), vec![]);
        assert_eq!(graph.ready_tasks(&HashSet::from([2]), 8), vec![
            (4, 0), (9, 0), (9, 1), (9, 2)
        ]);
        assert!(graph.ready_tasks(&HashSet::from([2, 4, 9]), 8).is_empty());
    }
}

#[cfg(test)]
mod narrow_stage_dependencies_tests {
    use super::*;

    #[test]
    fn long_unary_chain_is_valid_and_topologically_stable() {
        let stages = (0..4096_u64).map(|id| Stage {
            id, partitions: 1,
            dependencies: if id == 0 { vec![] } else { vec![id - 1] },
        }).collect();
        let graph = StageGraph::new(stages).unwrap();
        assert_eq!(graph.longest_dependency_chain(), 4096);
        assert_eq!(graph.topological_order().len(), 4096);
        assert_eq!(graph.ready(&HashSet::new()), vec![0]);
    }

    #[test]
    fn binary_and_wide_duplicate_dependencies_are_rejected() {
        for duplicates in [vec![0, 0], vec![0, 1, 0]] {
            let mut stages = vec![
                Stage { id: 0, dependencies: vec![], partitions: 1 },
                Stage { id: 1, dependencies: vec![], partitions: 1 },
            ];
            stages.push(Stage { id: 2, dependencies: duplicates, partitions: 1 });
            assert!(StageGraph::new(stages).is_err());
        }
    }
}

#[cfg(test)]
mod cached_critical_path_priorities_tests {
    use super::*;

    #[test]
    fn repeated_scheduler_polls_keep_deepest_path_and_stable_ties() {
        let graph = StageGraph::new(vec![
            Stage { id: 8, dependencies: vec![5], partitions: 1 },
            Stage { id: 5, dependencies: vec![3], partitions: 1 },
            Stage { id: 3, dependencies: vec![1], partitions: 1 },
            Stage { id: 1, dependencies: vec![], partitions: 1 },
            Stage { id: 2, dependencies: vec![], partitions: 1 },
            Stage { id: 6, dependencies: vec![2], partitions: 1 },
        ]).unwrap();
        for _ in 0..32 {
            assert_eq!(graph.ready_by_critical_path(&HashSet::new()), [1, 2]);
            assert_eq!(graph.ready_by_critical_path(&HashSet::from([1, 2])), [3, 6]);
            assert_eq!(graph.ready_by_critical_path(&HashSet::from([1, 2, 3])), [5, 6]);
        }
    }
}

#[cfg(test)]
mod inflight_stage_partition_scheduling_tests {
    use super::*;

    #[test]
    fn repeated_windows_exclude_dispatched_tasks_but_not_new_partitions() {
        let graph = StageGraph::new(vec![
            Stage { id: 3, dependencies: vec![1, 2], partitions: 2 },
            Stage { id: 1, dependencies: vec![], partitions: 3 },
            Stage { id: 2, dependencies: vec![], partitions: 3 },
        ]).unwrap();
        let mut completed = HashSet::new();
        let first = graph.ready_tasks(&completed, 4);
        assert_eq!(first.len(), 4);
        let running: HashSet<(u64, usize)> = first.iter().copied().collect();
        let next = graph.ready_tasks_excluding(&completed, &running, 10);
        assert_eq!(next.len(), 2);
        assert!(next.iter().all(|task| !running.contains(task)));
        assert!(next.iter().all(|(stage, _)| *stage != 3));
        assert!(graph.ready_tasks_excluding(&completed, &running, 0).is_empty());

        completed.insert(1);
        assert!(graph.ready_tasks_excluding(&completed, &running, 10)
            .iter().all(|(stage, _)| *stage == 2));
        completed.insert(2);
        assert_eq!(graph.ready_tasks_excluding(&completed, &HashSet::new(), 10),
            vec![(3, 0), (3, 1)]);
    }
}

#[cfg(test)]
mod precomputed_critical_path_order_regressions {
    use super::*;

    #[test]
    fn repeated_polls_preserve_priority_ties_and_dependency_gates() {
        let graph = StageGraph::new(vec![
            Stage { id: 70, dependencies: vec![10], partitions: 1 },
            Stage { id: 50, dependencies: vec![30], partitions: 1 },
            Stage { id: 10, dependencies: vec![], partitions: 1 },
            Stage { id: 40, dependencies: vec![20], partitions: 1 },
            Stage { id: 30, dependencies: vec![10], partitions: 1 },
            Stage { id: 20, dependencies: vec![], partitions: 1 },
            Stage { id: 60, dependencies: vec![], partitions: 1 },
        ]).unwrap();
        for _ in 0..100 {
            assert_eq!(graph.ready_by_critical_path(&HashSet::new()), [10, 20, 60]);
            assert_eq!(graph.ready_by_critical_path(&HashSet::from([10])), [20, 30, 60, 70]);
            assert_eq!(graph.ready_by_critical_path(&HashSet::from([10, 20])),
                [30, 40, 60, 70]);
        }
    }
}

#[cfg(test)]
mod empty_running_dispatch_tests {
    use super::*;

    #[test]
    fn empty_running_windows_keep_topology_and_limits() {
        let graph = StageGraph::new(vec![
            Stage { id: 8, dependencies: vec![3], partitions: 3 },
            Stage { id: 3, dependencies: vec![], partitions: 7 },
            Stage { id: 5, dependencies: vec![], partitions: 2 },
        ]).unwrap();
        let completed = HashSet::new();
        let running = HashSet::new();
        for bound in [0, 1, 3, 7, 9, 100] {
            assert_eq!(graph.ready_tasks_excluding(&completed, &running, bound),
                graph.ready_tasks(&completed, bound));
        }
        let pending = graph.ready_tasks(&completed, 100);
        assert_eq!(pending.len(), 9);
        assert!(pending.iter().all(|(id, _)| *id != 8));
        let completed = HashSet::from([3]);
        assert_eq!(graph.ready_tasks(&completed, 100).len(), 5);
    }
}

#[cfg(test)]
mod dense_running_partition_windows_regressions {
    use super::*;

    #[test]
    fn skips_large_contiguous_claimed_ranges_and_ignores_unknown_tasks() {
        let graph = StageGraph::new(vec![
            Stage { id: 11, dependencies: vec![], partitions: 1400 },
            Stage { id: 22, dependencies: vec![], partitions: 1000 },
        ]).unwrap();
        let running: HashSet<_> = (0..1399).map(|p| (11, p))
            .chain([(11, 99_999), (999, 0)]).collect();
        let tasks = graph.ready_tasks_excluding(&HashSet::new(), &running, 4);
        assert_eq!(tasks, [(11, 1399), (22, 0), (22, 1), (22, 2)]);
        assert_eq!(graph.ready_tasks_excluding(&HashSet::from([11]), &running, 3),
            [(22, 0), (22, 1), (22, 2)]);
    }

    #[test]
    fn dense_exclusions_match_reference_scan_at_varied_window_sizes() {
        let graph = StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![], partitions: 2200 },
            Stage { id: 2, dependencies: vec![], partitions: 1300 },
        ]).unwrap();
        let running = (0..2200).filter(|p| p % 5 != 0)
            .map(|p| (1, p))
            .chain((0..1300).filter(|p| p % 7 == 0).map(|p| (2, p)))
            .collect::<HashSet<_>>();
        for window in [1, 4, 13, 1024, 5000] {
            let expected = [(1, 2200), (2, 1300)].into_iter()
                .flat_map(|(stage, count)| (0..count).map(move |p| (stage, p)))
                .filter(|task| !running.contains(task)).take(window).collect::<Vec<_>>();
            assert_eq!(graph.ready_tasks_excluding(&HashSet::new(), &running, window),
                expected);
        }
    }
}

#[cfg(test)]
mod critical_path_partition_dispatch_tests {
    use super::*;

    fn graph() -> StageGraph {
        StageGraph::new(vec![
            Stage { id: 20, dependencies: vec![], partitions: 2 },
            Stage { id: 10, dependencies: vec![], partitions: 3 },
            Stage { id: 11, dependencies: vec![10], partitions: 1 },
            Stage { id: 12, dependencies: vec![11], partitions: 1 },
        ]).unwrap()
    }

    #[test]
    fn prioritizes_upstream_partitions_on_longest_chain() {
        let graph = graph();
        let completed = HashSet::new();
        assert_eq!(graph.ready_tasks(&completed, 3),
            vec![(20, 0), (20, 1), (10, 0)]);
        assert_eq!(graph.ready_tasks_by_critical_path(&completed, 3),
            vec![(10, 0), (10, 1), (10, 2)]);
        assert_eq!(graph.ready_tasks_by_critical_path(&HashSet::from([10]), 3),
            vec![(11, 0), (20, 0), (20, 1)]);
    }

    #[test]
    fn critical_windows_respect_running_exclusions_and_depth_barriers() {
        let graph = graph();
        let completed = HashSet::new();
        let running = HashSet::from([(10, 0), (10, 1), (20, 1)]);
        assert_eq!(graph.ready_tasks_by_critical_path_excluding(
            &completed, &running, 8), vec![(10, 2), (20, 0)]);
        assert_eq!(graph.ready_tasks_by_critical_path_excluding(
            &completed, &running, 0), Vec::<(u64, usize)>::new());
        // Exercise the sorted-range fast path used for heavily occupied windows.
        let big = StageGraph::new(vec![
            Stage { id: 1, dependencies: vec![], partitions: 1024 },
            Stage { id: 2, dependencies: vec![1], partitions: 1 },
        ]).unwrap();
        let exclusions = (0..900).map(|partition| (1, partition))
            .collect::<HashSet<_>>();
        assert_eq!(big.ready_tasks_by_critical_path_excluding(
            &HashSet::new(), &exclusions, 2), vec![(1, 900), (1, 901)]);
    }
}
