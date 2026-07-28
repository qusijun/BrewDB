//! Task execution shell.

use crate::errors::ExecutionError;
use crate::task::{TaskExecutionSummary, TaskRequest, TaskResult};

/// Worker-side request to execute one scheduled task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteTask {
    pub request: TaskRequest,
}

/// Worker-side execution outcome before reporting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskExecutionOutcome {
    pub result: TaskResult,
    pub summary: TaskExecutionSummary,
}

/// Worker task executor boundary.
pub trait TaskExecutor {
    fn execute_task(&self, request: ExecuteTask) -> Result<TaskExecutionOutcome, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};

    use crate::boundaries::BoundaryDescriptor;
    use crate::task::{TaskRequest, TaskRole};

    use super::ExecuteTask;

    #[test]
    fn execute_task_wraps_worker_request() {
        let command = ExecuteTask {
            request: TaskRequest {
                job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441100").unwrap(),
                stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655441101").unwrap(),
                task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655441102").unwrap(),
                attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655441103")
                    .unwrap(),
                stage_plan_id: "stage-plan-a".to_owned(),
                partition_id: 0,
                boundary: BoundaryDescriptor::for_kind(BoundaryKind::Exchange),
                dependencies: Vec::new(),
                role: TaskRole::Compute,
            },
        };

        assert_eq!(command.request.stage_plan_id, "stage-plan-a");
    }
}
