//! Stage graphs and execution planning contracts.

use brewdb_core::execution::BoundaryKind;
use brewdb_core::ids::{JobId, StageId};

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

/// A lightweight execution stage graph reference used during orchestration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageGraph {
    pub job_id: JobId,
    pub stages: Vec<StagePlan>,
}

impl StageGraph {
    pub fn new(job_id: JobId, stages: Vec<StagePlan>) -> Self {
        Self { job_id, stages }
    }

    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId};

    use super::{StageGraph, StageKind, StagePlan};

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
    }
}
