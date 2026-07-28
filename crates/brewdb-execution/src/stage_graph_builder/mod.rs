//! Top-level stage-graph builder pipeline contracts.

mod boundary_detection;
mod pipeline;
mod stage_split;
mod task_split;

pub use boundary_detection::{
    BoundaryAnchor, BoundaryDetectionInput, BoundaryDetectionOutput, BoundaryDetector,
};
pub use pipeline::{
    BuildStageGraph, PlanRoot, PlanRootDistribution, PlanRootOutput, StageGraphBuildPipeline,
    StageGraphBuildResult,
};
pub use stage_split::{SplitStages, StageSplitOutput, StageSplitter};
pub use task_split::{SplitTasks, TaskSplitOutput, TaskSplitter};

use crate::errors::ExecutionError;
use crate::plan::StageGraph;

/// Execution-side stage graph builder boundary.
pub trait StageGraphBuilder {
    fn build_stage_graph(&self, command: BuildStageGraph) -> Result<StageGraph, ExecutionError>;
}
