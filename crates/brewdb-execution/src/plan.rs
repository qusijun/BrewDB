//! Stage graphs and execution planning contracts.

use brewdb_core::execution::BoundaryKind;
use brewdb_core::ids::{JobId, StageId, TaskId};

use crate::boundaries::BoundaryDescriptor;
use crate::task::TaskDependency;

/// Stable execution stage kinds used by runtime planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageKind {
    Compute,
    Exchange,
    Materialize,
    Selection,
}

/// A lightweight execution stage plan shared with the runtime layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePlan {
    pub stage_id: StageId,
    pub kind: StageKind,
    pub boundary: Option<BoundaryKind>,
}

/// One execution task descriptor inside a stage graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskPlan {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub partition_id: u32,
    pub dependencies: Vec<TaskDependency>,
}

/// A lightweight execution stage graph reference used during orchestration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageGraph {
    pub job_id: JobId,
    pub stages: Vec<StagePlan>,
    pub tasks: Vec<TaskPlan>,
}

impl StageGraph {
    pub fn new(job_id: JobId, stages: Vec<StagePlan>) -> Self {
        Self {
            job_id,
            stages,
            tasks: Vec::new(),
        }
    }

    pub fn with_tasks(mut self, tasks: Vec<TaskPlan>) -> Self {
        self.tasks = tasks;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub fn stage_boundary(&self, stage_id: &StageId) -> Option<BoundaryDescriptor> {
        self.stages
            .iter()
            .find(|stage| &stage.stage_id == stage_id)
            .and_then(|stage| stage.boundary.map(BoundaryDescriptor::for_kind))
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId, TaskId};

    use crate::task::TaskDependency;

    use super::{StageGraph, StageKind, StagePlan, TaskPlan};

    #[test]
    fn stage_graph_tracks_stages() {
        let graph = StageGraph::new(
            JobId::parse_str("550e8400-e29b-41d4-a716-446655440300").unwrap(),
            vec![StagePlan {
                stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440301").unwrap(),
                kind: StageKind::Materialize,
                boundary: Some(BoundaryKind::Materialization),
            }],
        );

        assert!(!graph.is_empty());
        assert_eq!(graph.stages.len(), 1);
        assert!(graph.tasks.is_empty());
    }

    #[test]
    fn stage_graph_tracks_task_dependencies() {
        let stage_id = StageId::parse_str("550e8400-e29b-41d4-a716-446655440305").unwrap();
        let task = TaskPlan {
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440306").unwrap(),
            stage_id: stage_id.clone(),
            partition_id: 0,
            dependencies: vec![TaskDependency {
                upstream_stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440307")
                    .unwrap(),
                upstream_task_id: None,
                partition_id: 1,
            }],
        };
        let graph = StageGraph::new(
            JobId::parse_str("550e8400-e29b-41d4-a716-446655440304").unwrap(),
            vec![StagePlan {
                stage_id: stage_id.clone(),
                kind: StageKind::Exchange,
                boundary: Some(BoundaryKind::Exchange),
            }],
        )
        .with_tasks(vec![task]);

        assert_eq!(graph.tasks.len(), 1);
        assert!(graph.stage_boundary(&stage_id).is_some());
    }
}
