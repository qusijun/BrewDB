//! Task request and result contracts.

use brewdb_core::ids::{ArtifactId, JobId, StageId, TaskAttemptId, TaskId};

use crate::boundaries::BoundaryDescriptor;

/// Useful worker-visible task roles in Phase 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskRole {
    Compute,
    ExchangeProducer,
    ExchangeConsumer,
    AppendMaterialize,
    RewriteMaterialize,
    SelectionMaterialize,
}

/// One upstream partition dependency required by a task.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskDependency {
    pub upstream_stage_id: StageId,
    pub upstream_task_id: Option<TaskId>,
    pub partition_id: u32,
}

/// Worker-targeted execution payload shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRequest {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: TaskAttemptId,
    pub plan_segment_id: String,
    pub partition_id: u32,
    pub boundary: BoundaryDescriptor,
    pub dependencies: Vec<TaskDependency>,
    pub role: TaskRole,
}

impl TaskRequest {
    pub fn is_ready_without_dependencies(&self) -> bool {
        self.dependencies.is_empty()
    }
}

/// Lightweight execution summary returned by workers.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TaskExecutionSummary {
    pub rows_out: Option<u64>,
    pub bytes_out: Option<u64>,
    pub spilled: bool,
}

/// Boundary-bearing task completion result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResult {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: TaskAttemptId,
    pub success: bool,
    pub summary: TaskExecutionSummary,
    pub produced_artifact_ids: Vec<ArtifactId>,
}

impl TaskResult {
    pub fn carries_boundary_artifacts(&self) -> bool {
        !self.produced_artifact_ids.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{ArtifactId, JobId, StageId, TaskAttemptId, TaskId};

    use crate::boundaries::BoundaryDescriptor;

    use super::{TaskDependency, TaskExecutionSummary, TaskRequest, TaskResult, TaskRole};

    #[test]
    fn request_without_dependencies_is_immediately_runnable() {
        let request = TaskRequest {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440400").unwrap(),
            stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440401").unwrap(),
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440402").unwrap(),
            attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655440403").unwrap(),
            plan_segment_id: "plan-segment-0".to_owned(),
            partition_id: 0,
            boundary: BoundaryDescriptor::for_kind(BoundaryKind::Exchange),
            dependencies: Vec::new(),
            role: TaskRole::Compute,
        };

        assert!(request.is_ready_without_dependencies());
    }

    #[test]
    fn task_result_exposes_boundary_artifacts() {
        let result = TaskResult {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440410").unwrap(),
            stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440411").unwrap(),
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440412").unwrap(),
            attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655440413").unwrap(),
            success: true,
            summary: TaskExecutionSummary {
                rows_out: Some(10),
                bytes_out: Some(1024),
                spilled: false,
            },
            produced_artifact_ids: vec![
                ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655440414").unwrap(),
            ],
        };

        let dependency = TaskDependency {
            upstream_stage_id: result.stage_id.clone(),
            upstream_task_id: Some(result.task_id.clone()),
            partition_id: 0,
        };

        assert!(result.carries_boundary_artifacts());
        assert_eq!(dependency.partition_id, 0);
    }
}
