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
}

impl StageGraph {
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
            let mut seen = HashSet::new();
            for dep in &stage.dependencies {
                if !seen.insert(*dep) {
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
        while let Some(position) = ready.pop_front() {
            order.push(stages[position].id);
            order_indices.push(position);
            for &successor in &successors[position] {
                indegree[successor] -= 1;
                if indegree[successor] == 0 {
                    ready.push_back(successor);
                }
            }
        }
        if order.len() != stages.len() {
            return Err(Error::Execution("execution stage graph contains a cycle".into()));
        }
        Ok(Self { stages, order, order_indices })
    }

    pub fn topological_order(&self) -> &[u64] {
        &self.order
    }

    pub fn ready(&self, completed: &HashSet<u64>) -> Vec<u64> {
        self.order_indices.iter().filter_map(|&index| {
            let stage = &self.stages[index];
            (!completed.contains(&stage.id) &&
                stage.dependencies.iter().all(|dependency| completed.contains(dependency)))
                .then_some(stage.id)
        }).collect()
    }

    /// Enumerate runnable partition tasks in topological order without exceeding
    /// the caller's scheduling window. Completed stages are not rescheduled.
    pub fn ready_tasks(&self, completed: &HashSet<u64>, max_tasks: usize) -> Vec<(u64, usize)> {
        let mut tasks = Vec::new();
        for &index in &self.order_indices {
            let stage = &self.stages[index];
            if completed.contains(&stage.id)
                || !stage.dependencies.iter().all(|dependency| completed.contains(dependency)) {
                continue;
            }
            for partition in 0..stage.partitions {
                if tasks.len() == max_tasks { return tasks; }
                tasks.push((stage.id, partition));
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
        let by_id: HashMap<u64, &Stage> = self.stages.iter().map(|stage| (stage.id, stage)).collect();
        let mut depth = HashMap::new();
        for id in &self.order {
            let stage = by_id[id];
            let longest_parent = stage.dependencies.iter()
                .map(|parent| depth.get(parent).copied().unwrap_or(0))
                .max().unwrap_or(0);
            depth.insert(*id, longest_parent + 1);
        }
        depth.values().copied().max().unwrap_or(0)
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
