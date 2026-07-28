//! High-level execution graph build pipeline shell.

use brewdb_core::ids::JobId;

use crate::errors::ExecutionError;
use crate::plan::StageGraph;
use crate::stage_graph_builder::{
    BoundaryDetectionInput, BoundaryDetectionOutput, SplitStages, SplitTasks, StageSplitOutput,
    TaskSplitOutput,
};

/// Input distribution hint attached to the physical plan root before stage slicing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlanRootDistribution {
    Singleton,
    HashPartitioned,
    Broadcast,
    Unknown,
}

/// Output shape produced by the physical plan root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PlanRootOutput {
    Rows,
    ExchangeStream,
    StagedArtifacts,
    CandidateSet,
}

/// Minimal root descriptor for the physical plan entering the stage-graph build pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanRoot {
    pub plan_root_id: String,
    pub distribution: PlanRootDistribution,
    pub output: PlanRootOutput,
}

/// Coordinator-provided planning inputs for execution slicing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildStageGraph {
    pub job_id: JobId,
    pub plan_root: PlanRoot,
}

/// Pipeline result shell, keeping intermediate outputs visible at the architecture boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageGraphBuildResult {
    pub boundaries: BoundaryDetectionOutput,
    pub stage_split: StageSplitOutput,
    pub task_split: TaskSplitOutput,
    pub stage_graph: StageGraph,
}

/// Top-level execution graph build pipeline.
pub trait StageGraphBuildPipeline {
    fn detect_boundaries(
        &self,
        input: BoundaryDetectionInput,
    ) -> Result<BoundaryDetectionOutput, ExecutionError>;

    fn split_stages(&self, input: SplitStages) -> Result<StageSplitOutput, ExecutionError>;

    fn split_tasks(&self, input: SplitTasks) -> Result<TaskSplitOutput, ExecutionError>;

    fn assemble_stage_graph(
        &self,
        result: StageGraphBuildResult,
    ) -> Result<StageGraph, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::JobId;

    use super::{BuildStageGraph, PlanRoot, PlanRootDistribution, PlanRootOutput};

    #[test]
    fn build_command_carries_job_and_plan_root() {
        let command = BuildStageGraph {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440801").unwrap(),
            plan_root: PlanRoot {
                plan_root_id: "physical-root-0".to_owned(),
                distribution: PlanRootDistribution::Singleton,
                output: PlanRootOutput::Rows,
            },
        };

        assert_eq!(command.plan_root.plan_root_id, "physical-root-0");
    }
}
