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
}

impl StageGraph {
    pub fn new(stages: Vec<Stage>) -> Result<Self> {
        if stages.len() > 4096 {
            return Err(Error::Execution("execution graph exceeds 4096 stages".into()));
        }
        let mut index = HashMap::new();
        for (position, stage) in stages.iter().enumerate() {
            if stage.partitions == 0 {
                return Err(Error::Execution("stage must have at least one partition".into()));
            }
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
        while let Some(position) = ready.pop_front() {
            order.push(stages[position].id);
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
        Ok(Self { stages, order })
    }

    pub fn topological_order(&self) -> &[u64] {
        &self.order
    }

    pub fn ready(&self, completed: &HashSet<u64>) -> Vec<u64> {
        self.stages.iter().filter(|stage| {
            !completed.contains(&stage.id) &&
                stage.dependencies.iter().all(|dependency| completed.contains(dependency))
        }).map(|stage| stage.id).collect()
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
