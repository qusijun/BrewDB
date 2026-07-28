//! Task-attempt runtime truth records.

use brewdb_core::ids::{JobId, StageId, TaskAttemptId, TaskId};
use brewdb_core::state::TaskAttemptState;

/// Persisted lifecycle truth for one concrete task attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAttemptRecord {
    pub task_attempt_id: TaskAttemptId,
    pub job_id: JobId,
    pub stage_id: StageId,
    pub task_id: TaskId,
    pub state: TaskAttemptState,
    pub worker_id: Option<String>,
}
