//! Worker-to-coordinator protocol shell.

use crate::task::{TaskExecutionSummary, TaskResult};

/// Progress DTO shell emitted before terminal task completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskProgressWire {
    pub summary: TaskExecutionSummary,
}

/// Terminal task-result DTO shell emitted by workers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskResultWire {
    pub result: TaskResult,
}

/// Top-level worker envelope reserved for future transport metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerTaskEnvelope {
    Progress(TaskProgressWire),
    Result(TaskResultWire),
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{ArtifactId, JobId, StageId, TaskAttemptId, TaskId};

    use crate::task::{TaskExecutionSummary, TaskResult};

    use super::{TaskProgressWire, TaskResultWire, WorkerTaskEnvelope};

    #[test]
    fn worker_envelope_keeps_progress_and_result_shapes() {
        let progress = WorkerTaskEnvelope::Progress(TaskProgressWire {
            summary: TaskExecutionSummary {
                rows_out: Some(10),
                bytes_out: Some(512),
                spilled: false,
            },
        });
        let result = WorkerTaskEnvelope::Result(TaskResultWire {
            result: TaskResult {
                job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441310").unwrap(),
                stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655441311").unwrap(),
                task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655441312").unwrap(),
                attempt_id: TaskAttemptId::parse_str("550e8400-e29b-41d4-a716-446655441313")
                    .unwrap(),
                success: true,
                summary: TaskExecutionSummary {
                    rows_out: Some(10),
                    bytes_out: Some(512),
                    spilled: false,
                },
                produced_artifact_ids: vec![
                    ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655441314").unwrap(),
                ],
            },
        });

        assert!(matches!(progress, WorkerTaskEnvelope::Progress(_)));
        assert!(matches!(result, WorkerTaskEnvelope::Result(_)));
    }
}
