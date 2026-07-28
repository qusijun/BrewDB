//! Stage-output writing shell.

use brewdb_core::artifacts::{ArtifactBundleKind, ArtifactRef};
use brewdb_core::ids::JobId;

use crate::errors::ExecutionError;

/// Request to persist one staged output group from worker-local execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteStageOutput {
    pub job_id: JobId,
    pub bundle_kind: ArtifactBundleKind,
    pub stage_plan_id: String,
    pub partition_id: u32,
}

/// Result of staged output persistence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageOutputWriteResult {
    pub written_artifacts: Vec<ArtifactRef>,
}

/// Worker-side stage-output writer boundary.
pub trait StageOutputWriter {
    fn write_stage_output(
        &self,
        request: WriteStageOutput,
    ) -> Result<StageOutputWriteResult, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::artifacts::ArtifactBundleKind;
    use brewdb_core::ids::JobId;

    use super::WriteStageOutput;

    #[test]
    fn stage_output_request_keeps_stage_plan_reference() {
        let request = WriteStageOutput {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441110").unwrap(),
            bundle_kind: ArtifactBundleKind::Append,
            stage_plan_id: "stage-plan-b".to_owned(),
            partition_id: 3,
        };

        assert_eq!(request.stage_plan_id, "stage-plan-b");
        assert_eq!(request.partition_id, 3);
    }
}
