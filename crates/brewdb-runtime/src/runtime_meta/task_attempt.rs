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

impl TaskAttemptRecord {
    pub fn new(
        task_attempt_id: TaskAttemptId,
        job_id: JobId,
        stage_id: StageId,
        task_id: TaskId,
    ) -> Self {
        Self {
            task_attempt_id,
            job_id,
            stage_id,
            task_id,
            state: TaskAttemptState::Pending,
            worker_id: None,
        }
    }

    pub fn with_worker(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }
}
