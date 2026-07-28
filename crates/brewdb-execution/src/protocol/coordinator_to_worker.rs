//! Coordinator-to-worker protocol shell.

use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};

use crate::boundaries::BoundaryDescriptor;
use crate::task::{TaskDependency, TaskRole};

/// Internal protocol version shell reserved for future transport negotiation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtocolVersion {
    V1,
}

/// Wire-visible reference to one stage-scoped DataFusion plan slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePlanRef {
    pub stage_id: StageId,
    pub stage_plan_id: String,
}

/// Coordinator-to-worker task request DTO shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRequestWire {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: TaskAttemptId,
    pub stage_plan: StagePlanRef,
    pub partition_id: u32,
    pub boundary: BoundaryDescriptor,
    pub dependencies: Vec<TaskDependency>,
    pub role: TaskRole,
}

/// Top-level coordinator envelope reserved for future transport metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatorTaskEnvelope {
    pub version: ProtocolVersion,
    pub request: TaskRequestWire,
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};

    use crate::boundaries::BoundaryDescriptor;
    use crate::task::TaskRole;

    use super::{CoordinatorTaskEnvelope, ProtocolVersion, StagePlanRef, TaskRequestWire};

    #[test]
    fn task_request_wire_keeps_stage_plan_reference() {
        let stage_id = StageId::parse_str("550e8400-e29b-41d4-a716-446655441300").unwrap();
        let wire = TaskRequestWire {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441301").unwrap(),
            stage_id: stage_id.clone(),
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655441302").unwrap(),
            attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655441303").unwrap(),
            stage_plan: StagePlanRef {
                stage_id,
                stage_plan_id: "stage-plan-a".to_owned(),
            },
            partition_id: 2,
            boundary: BoundaryDescriptor::for_kind(BoundaryKind::Exchange),
            dependencies: Vec::new(),
            role: TaskRole::Compute,
        };
        let envelope = CoordinatorTaskEnvelope {
            version: ProtocolVersion::V1,
            request: wire,
        };

        assert_eq!(envelope.request.stage_plan.stage_plan_id, "stage-plan-a");
        assert_eq!(envelope.version, ProtocolVersion::V1);
    }
}
