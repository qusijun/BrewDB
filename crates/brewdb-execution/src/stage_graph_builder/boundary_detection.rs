//! Boundary detection shell for execution graph construction.

use brewdb_core::execution::BoundaryKind;
use brewdb_core::ids::JobId;

use crate::errors::ExecutionError;
use crate::stage_graph_builder::PlanRoot;

/// One detected boundary anchored at a physical plan location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryAnchor {
    pub plan_root_id: String,
    pub boundary_kind: BoundaryKind,
}

/// Input to boundary detection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundaryDetectionInput {
    pub job_id: JobId,
    pub plan_root: PlanRoot,
}

/// Output of boundary detection before stage splitting.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct BoundaryDetectionOutput {
    pub detected_boundaries: Vec<BoundaryAnchor>,
}

/// Boundary detector boundary.
pub trait BoundaryDetector {
    fn detect_boundaries(
        &self,
        input: BoundaryDetectionInput,
    ) -> Result<BoundaryDetectionOutput, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::JobId;

    use crate::stage_graph_builder::{PlanRoot, PlanRootDistribution, PlanRootOutput};

    use super::{BoundaryAnchor, BoundaryDetectionInput, BoundaryDetectionOutput};

    #[test]
    fn boundary_detection_output_tracks_stage_anchors() {
        let input = BoundaryDetectionInput {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440810").unwrap(),
            plan_root: PlanRoot {
                plan_root_id: "plan-root-a".to_owned(),
                distribution: PlanRootDistribution::Broadcast,
                output: PlanRootOutput::ExchangeStream,
            },
        };
        let output = BoundaryDetectionOutput {
            detected_boundaries: vec![BoundaryAnchor {
                plan_root_id: input.plan_root.plan_root_id.clone(),
                boundary_kind: BoundaryKind::Exchange,
            }],
        };

        assert_eq!(output.detected_boundaries.len(), 1);
    }
}
