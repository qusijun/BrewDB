//! Top-level dispatch orchestration contracts.

use brewdb_core::ids::{JobId, StageId};
use brewdb_execution::plan::StageGraph;
use brewdb_execution::task::TaskRequest;

use crate::errors::RuntimeError;
use crate::runtime_meta::{StageRecord, TaskAttemptRecord};
use crate::scheduler::{DispatchDecision, SchedulingSnapshot, WorkerSlot};

/// Coordinator command to register a full execution graph for dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterExecutionGraph {
    pub job_id: JobId,
    pub stage_graph: StageGraph,
}

/// Coordinator command to request the next dispatch wave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchWave {
    pub job_id: JobId,
    pub snapshot: SchedulingSnapshot,
    pub workers: Vec<WorkerSlot>,
}

/// One dispatch batch emitted by the coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DispatchBatch {
    pub stage_records: Vec<StageRecord>,
    pub task_attempt_records: Vec<TaskAttemptRecord>,
    pub task_requests: Vec<TaskRequest>,
    pub decisions: Vec<DispatchDecision>,
}

/// Dispatcher boundary for runtime orchestration.
pub trait DispatcherService {
    fn register_execution_graph(
        &self,
        command: RegisterExecutionGraph,
    ) -> Result<Vec<StageRecord>, RuntimeError>;

    fn dispatch_wave(&self, command: DispatchWave) -> Result<DispatchBatch, RuntimeError>;
}

impl RegisterExecutionGraph {
    pub fn stage_ids(&self) -> Vec<StageId> {
        self.stage_graph
            .stages
            .iter()
            .map(|stage| stage.stage_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId};
    use brewdb_execution::plan::{StageGraph, StageKind, StagePlan};

    use super::RegisterExecutionGraph;

    #[test]
    fn register_command_exposes_stage_identity() {
        let command = RegisterExecutionGraph {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440610").unwrap(),
            stage_graph: StageGraph::new(
                JobId::parse_str("550e8400-e29b-41d4-a716-446655440610").unwrap(),
                vec![StagePlan {
                    stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440611").unwrap(),
                    kind: StageKind::Exchange,
                    boundary: Some(BoundaryKind::Exchange),
                }],
            ),
        };

        assert_eq!(command.stage_ids().len(), 1);
    }
}
