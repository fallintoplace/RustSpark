//! Native execution stage graph for local and future distributed scheduling.
use std::{cmp::Reverse, collections::BinaryHeap};

use crate::{plan::Plan, Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageKind {
    Scan,
    Filter,
    Project,
    Aggregate,
    Sort,
    Limit,
    Distinct,
    HashJoin,
    CrossJoin,
    UnionAll,
    Intersect,
    Except,
}

impl StageKind {
    pub fn is_blocking(self) -> bool {
        matches!(self, Self::Aggregate | Self::Sort | Self::Distinct | Self::HashJoin | Self::CrossJoin | Self::Intersect | Self::Except)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stage {
    pub id: usize,
    pub kind: StageKind,
    /// Upstream stage identifiers.
    pub inputs: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageGraph {
    stages: Vec<Stage>,
}

impl StageGraph {
    pub fn new(stages: Vec<Stage>) -> Result<Self> {
        // Enforce the same bound as logical-plan lowering for direct graph users.
        if stages.len() > 4096 {
            return Err(Error::Execution("native stage graph exceeds safety limits".into()));
        }
        let mut present = vec![false; stages.len()];
        for stage in &stages {
            if stage.id >= stages.len() || std::mem::replace(&mut present[stage.id], true) {
                return Err(Error::Schema("duplicate or invalid stage identifier".into()));
            }
        }
        for stage in &stages {
            if stage.inputs.len() >= 2
                && stage.inputs.windows(2).any(|edges| edges[0] == edges[1]) {
                // Valid operators have at most two inputs.
                return Err(Error::Schema("stage has duplicate dependencies".into()));
            }
            if stage.inputs.iter().any(|&dependency| {
                dependency >= present.len() || !present[dependency]
            }) {
                return Err(Error::Schema("stage depends on an unknown stage".into()));
            }
            let required_inputs = match stage.kind {
                StageKind::Scan => 0,
                StageKind::Filter | StageKind::Project | StageKind::Aggregate |
                StageKind::Sort | StageKind::Limit | StageKind::Distinct => 1,
                StageKind::HashJoin | StageKind::CrossJoin | StageKind::UnionAll | StageKind::Intersect |
                StageKind::Except => 2,
            };
            if stage.inputs.len() != required_inputs {
                return Err(Error::Schema("invalid physical stage input count".into()));
            }
        }
        let graph = Self { stages };
        graph.execution_order()?;
        Ok(graph)
    }

    pub fn stages(&self) -> &[Stage] { &self.stages }

    pub fn root(&self) -> Option<usize> {
        let mut referenced = vec![false; self.stages.len()];
        for stage in &self.stages {
            for &input in &stage.inputs { referenced[input] = true; }
        }
        let mut roots = self.stages.iter().filter(|stage| !referenced[stage.id]);
        match (roots.next(), roots.next()) {
            (Some(root), None) => Some(root.id),
            _ => None,
        }
    }

    /// Deterministic topological order. Rejects cycles before any task starts.
    pub fn execution_order(&self) -> Result<Vec<usize>> {
        let mut degrees = vec![0_usize; self.stages.len()];
        let mut children = vec![Vec::new(); self.stages.len()];
        for stage in &self.stages {
            for &input in &stage.inputs {
                degrees[stage.id] += 1;
                children[input].push(stage.id);
            }
        }
        let mut ready = BinaryHeap::new();
        for (id, degree) in degrees.iter().enumerate() {
            if *degree == 0 { ready.push(Reverse(id)); }
        }
        let mut ordered = Vec::with_capacity(self.stages.len());
        while let Some(Reverse(id)) = ready.pop() {
            ordered.push(id);
            for &child in &children[id] {
                degrees[child] -= 1;
                if degrees[child] == 0 { ready.push(Reverse(child)); }
            }
        }
        if ordered.len() != self.stages.len() {
            return Err(Error::Execution("native execution stage graph contains a cycle".into()));
        }
        Ok(ordered)
    }

    /// Lower the existing native logical tree to a physical dependency graph.
    /// Exchange boundaries can be assigned to its blocking stages later.
    pub fn from_plan(plan: &Plan) -> Result<Self> {
        fn visit(plan: &Plan, stages: &mut Vec<Stage>, depth: usize) -> Result<usize> {
            if depth >= 64 || stages.len() >= 4096 {
                return Err(Error::Execution("native stage graph exceeds safety limits".into()));
            }
            let (kind, inputs) = match plan {
                Plan::Scan { .. } => (StageKind::Scan, vec![]),
                Plan::Filter { input, .. } =>
                    (StageKind::Filter, vec![visit(input, stages, depth + 1)?]),
                Plan::Project { input, .. } =>
                    (StageKind::Project, vec![visit(input, stages, depth + 1)?]),
                Plan::Aggregate { input, .. } =>
                    (StageKind::Aggregate, vec![visit(input, stages, depth + 1)?]),
                Plan::Sort { input, .. } =>
                    (StageKind::Sort, vec![visit(input, stages, depth + 1)?]),
                Plan::Limit { input, .. } =>
                    (StageKind::Limit, vec![visit(input, stages, depth + 1)?]),
                Plan::Distinct { input } =>
                    (StageKind::Distinct, vec![visit(input, stages, depth + 1)?]),
                Plan::Join { left, right, .. } => (
                    StageKind::HashJoin,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::CrossJoin { left, right } => (
                    StageKind::CrossJoin,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::UnionAll { left, right } => (
                    StageKind::UnionAll,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::Intersect { left, right } => (
                    StageKind::Intersect,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::IntersectAll { left, right } => (
                    StageKind::Intersect,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::ExceptAll { left, right } => (
                    StageKind::Except,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
                Plan::Except { left, right } => (
                    StageKind::Except,
                    vec![visit(left, stages, depth + 1)?, visit(right, stages, depth + 1)?],
                ),
            };
            let id = stages.len();
            if id >= 4096 {
                return Err(Error::Execution("native stage graph exceeds safety limits".into()));
            }
            stages.push(Stage { id, kind, inputs });
            Ok(id)
        }
        let mut stages = Vec::new();
        visit(plan, &mut stages, 0)?;
        Self::new(stages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::JoinKind;

    #[test]
    fn lowering_builds_a_deterministic_two_input_stage_dag() {
        let plan = Plan::Join {
            left: Box::new(Plan::scan("left")),
            right: Box::new(Plan::scan("right")),
            left_key: "id".into(),
            right_key: "id".into(),
            kind: JoinKind::Inner,
        };
        let graph = StageGraph::from_plan(&plan).unwrap();
        assert_eq!(graph.execution_order().unwrap(), vec![0, 1, 2]);
        assert_eq!(graph.root(), Some(2));
        assert_eq!(graph.stages()[2].inputs, vec![0, 1]);
        assert!(graph.stages()[2].kind.is_blocking());
    }

    #[test]
    fn intersect_stage_has_two_inputs_and_blocks() {
        let plan = Plan::Intersect {
            left: Box::new(Plan::scan("a")),
            right: Box::new(Plan::scan("b")),
        };
        let graph = StageGraph::from_plan(&plan).unwrap();
        let root = graph.root().unwrap();
        assert_eq!(graph.stages()[root].inputs, vec![0, 1]);
        assert_eq!(graph.stages()[root].kind, StageKind::Intersect);
        assert!(graph.stages()[root].kind.is_blocking());
    }

    #[test]
    fn except_stage_has_two_inputs_and_blocks() {
        let plan = Plan::Except {
            left: Box::new(Plan::scan("a")),
            right: Box::new(Plan::scan("b")),
        };
        let graph = StageGraph::from_plan(&plan).unwrap();
        let root = graph.root().unwrap();
        assert_eq!(graph.stages()[root].inputs, vec![0, 1]);
        assert_eq!(graph.stages()[root].kind, StageKind::Except);
        assert!(graph.stages()[root].kind.is_blocking());
    }

    #[test]
    fn rejects_cycles_and_unknown_input_stages() {
        let graph = vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![1] },
            Stage { id: 1, kind: StageKind::Filter, inputs: vec![0] },
        ];
        assert!(StageGraph::new(graph).is_err());
        assert!(StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![2] },
        ]).is_err());
    }
}

#[cfg(test)]
mod duplicate_stage_edges_tests {
    use super::*;

    #[test]
    fn rejects_repeated_inputs_to_a_single_stage() {
        let result = StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
            Stage { id: 1, kind: StageKind::Project, inputs: vec![0, 0] },
        ]);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod stage_graph_invariant_consolidation_tests {
    use super::*;
    #[test]
    fn root_is_derived_from_edges_not_caller_order() {
        let graph = StageGraph::new(vec![
            Stage { id: 2, kind: StageKind::Filter, inputs: vec![1] },
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
            Stage { id: 1, kind: StageKind::Filter, inputs: vec![0] },
        ]).unwrap();
        assert_eq!(graph.root(), Some(2));
        assert_eq!(graph.execution_order().unwrap(), vec![0, 1, 2]);
    }
    #[test]
    fn rejects_malformed_operator_input_arity() {
        assert!(StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![1] },
            Stage { id: 1, kind: StageKind::Scan, inputs: vec![] },
        ]).is_err());
        assert!(StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
            Stage { id: 1, kind: StageKind::HashJoin, inputs: vec![0] },
        ]).is_err());
    }
}

#[cfg(test)]
mod bitmap_stage_validation_tests {
    use super::*;
    #[test]
    fn accepts_nonsequential_stage_order_but_rejects_duplicate_ids() {
        let valid = vec![
            Stage { id: 1, kind: StageKind::Filter, inputs: vec![0] },
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
        ];
        assert!(StageGraph::new(valid.clone()).is_ok());
        let mut invalid = valid;
        invalid[0].id = 0;
        assert!(StageGraph::new(invalid).is_err());
        assert!(StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
            Stage { id: 1, kind: StageKind::HashJoin, inputs: vec![0, 0] },
        ]).is_err());
    }
}

#[cfg(test)]
mod dense_root_lookup_tests {
    use super::*;
    #[test]
    fn root_detection_preserves_empty_multiple_and_nonordered_graphs() {
        assert_eq!(StageGraph::new(vec![]).unwrap().root(), None);
        let graph = StageGraph::new(vec![
            Stage { id: 1, kind: StageKind::Filter, inputs: vec![0] },
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
        ]).unwrap();
        assert_eq!(graph.root(), Some(1));
        let multiple = StageGraph::new(vec![
            Stage { id: 1, kind: StageKind::Scan, inputs: vec![] },
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
        ]).unwrap();
        assert_eq!(multiple.root(), None);
    }
}

#[cfg(test)]
mod native_direct_stage_budget_tests {
    use super::*;

    #[test]
    fn direct_stage_graphs_cannot_bypass_lowering_limit() {
        let oversized = (0..4097).map(|id| Stage {
            id, kind: StageKind::Scan, inputs: vec![],
        }).collect();
        assert!(StageGraph::new(oversized).is_err());
        let accepted = StageGraph::new(vec![
            Stage { id: 0, kind: StageKind::Scan, inputs: vec![] },
        ]).unwrap();
        assert_eq!(accepted.execution_order().unwrap(), [0]);
    }
}

#[cfg(test)]
mod cross_join_stage_kind_tests {
    use super::*;

    #[test]
    fn cross_join_is_not_mislabeled_as_hash_join() {
        let plan = Plan::CrossJoin {
            left: Box::new(Plan::scan("left")),
            right: Box::new(Plan::scan("right")),
        };
        let graph = StageGraph::from_plan(&plan).unwrap();
        let root = &graph.stages()[graph.root().unwrap()];
        assert_eq!(root.kind, StageKind::CrossJoin);
        assert_eq!(root.inputs.len(), 2);
        assert!(root.kind.is_blocking());
    }
}
