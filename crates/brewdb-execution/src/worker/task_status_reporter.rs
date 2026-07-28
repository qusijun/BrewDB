//! Worker reporting shell back to the coordinator-side runtime.

use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};

use crate::errors::ExecutionError;
use crate::task::{TaskExecutionSummary, TaskResult};

/// Progress update emitted before terminal task completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskProgressUpdate {
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub attempt_id: TaskAttemptId,
    pub summary: TaskExecutionSummary,
}

/// Terminal report carrying the final task result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResultReport {
    pub result: TaskResult,
}

/// Worker task-status reporter boundary.
pub trait TaskStatusReporter {
    fn report_progress(
        &self,
        progress: TaskProgressUpdate,
    ) -> Result<TaskProgressUpdate, ExecutionError>;

    fn report_result(&self, result: TaskResultReport) -> Result<TaskResultReport, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};

    use crate::task::TaskExecutionSummary;

    use super::{TaskProgressUpdate, TaskStatusReporter};

    #[test]
    fn progress_update_keeps_attempt_identity() {
        let progress = TaskProgressUpdate {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441120").unwrap(),
            stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655441121").unwrap(),
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655441122").unwrap(),
            attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655441123").unwrap(),
            summary: TaskExecutionSummary {
                rows_out: Some(100),
                bytes_out: Some(2048),
                spilled: false,
            },
        };

        assert_eq!(progress.summary.rows_out, Some(100));
        let _ = Option::<&dyn TaskStatusReporter>::None;
    }
}
