//! Task split shell for execution graph construction.

use brewdb_core::ids::{JobId, TaskId};

use crate::errors::ExecutionError;
use crate::plan::TaskPlan;
use crate::stage_graph_builder::StageSplitOutput;

/// Input to the task splitting phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitTasks {
    pub job_id: JobId,
    pub stage_split: StageSplitOutput,
}

/// Output of task splitting before final graph assembly.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TaskSplitOutput {
    pub task_plans: Vec<TaskPlan>,
    pub root_task_ids: Vec<TaskId>,
}

/// Task splitter boundary.
pub trait TaskSplitter {
    fn split_tasks(&self, input: SplitTasks) -> Result<TaskSplitOutput, ExecutionError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{JobId, StageId, TaskId};

    use crate::plan::TaskPlan;

    use super::{SplitTasks, TaskSplitOutput};
    use crate::stage_graph_builder::StageSplitOutput;

    #[test]
    fn task_split_output_carries_task_shells() {
        let input = SplitTasks {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440830").unwrap(),
            stage_split: StageSplitOutput::default(),
        };
        let task_id = TaskId::parse_str("550e8400-e29b-41d4-a716-446655440831").unwrap();
        let output = TaskSplitOutput {
            task_plans: vec![TaskPlan {
                task_id: task_id.clone(),
                stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440832").unwrap(),
                partition_id: 0,
                dependencies: Vec::new(),
            }],
            root_task_ids: vec![task_id],
        };

        assert!(input.stage_split.stage_plans.is_empty());
        assert_eq!(output.task_plans.len(), 1);
    }
}
