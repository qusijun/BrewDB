//! Top-level stage-graph builder contracts.

use brewdb_core::ids::JobId;

use crate::errors::ExecutionError;
use crate::plan::StageGraph;

/// Coordinator-provided planning inputs for execution slicing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildStageGraph {
    pub job_id: JobId,
    pub fragment_root_id: String,
}

/// Execution-side stage graph builder boundary.
pub trait StageGraphBuilder {
    fn build_stage_graph(&self, command: BuildStageGraph) -> Result<StageGraph, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::JobId;

    use super::BuildStageGraph;

    #[test]
    fn build_command_carries_job_and_fragment_identity() {
        let command = BuildStageGraph {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440600").unwrap(),
            fragment_root_id: "physical-root-0".to_owned(),
        };

        assert_eq!(command.fragment_root_id, "physical-root-0");
        assert_eq!(
            command.job_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440600"
        );
    }
}
