//! Stage split shell for execution graph construction.

use brewdb_core::ids::{JobId, StageId};

use crate::errors::ExecutionError;
use crate::plan::StagePlan;
use crate::stage_graph_builder::BoundaryDetectionOutput;

/// Input to the stage splitting phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitStages {
    pub job_id: JobId,
    pub boundary_detection: BoundaryDetectionOutput,
}

/// Output of stage splitting before task splitting.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct StageSplitOutput {
    pub stage_plans: Vec<StagePlan>,
    pub source_stage_ids: Vec<StageId>,
}

/// Stage splitter boundary.
pub trait StageSplitter {
    fn split_stages(&self, input: SplitStages) -> Result<StageSplitOutput, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId};

    use crate::plan::{StageKind, StagePlan};

    use super::{SplitStages, StageSplitOutput};
    use crate::stage_graph_builder::BoundaryDetectionOutput;

    #[test]
    fn stage_split_output_carries_stage_shells() {
        let input = SplitStages {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440820").unwrap(),
            boundary_detection: BoundaryDetectionOutput::default(),
        };
        let stage_id = StageId::parse_str("550e8400-e29b-41d4-a716-446655440821").unwrap();
        let output = StageSplitOutput {
            stage_plans: vec![StagePlan {
                stage_id: stage_id.clone(),
                kind: StageKind::Compute,
                boundary: Some(BoundaryKind::Exchange),
            }],
            source_stage_ids: vec![stage_id],
        };

        assert!(input.boundary_detection.detected_boundaries.is_empty());
        assert_eq!(output.stage_plans.len(), 1);
    }
}
