//! Shared lifecycle state enums.

/// User-visible lifecycle state for one admitted job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JobState {
    Pending,
    Planning,
    Running,
    WaitingResource,
    Committing,
    Aborting,
    Succeeded,
    Failed,
    Canceled,
}

impl JobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

/// Lifecycle state for one distributed execution stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StageState {
    Pending,
    Schedulable,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl StageState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

/// Lifecycle state for one concrete task attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TaskAttemptState {
    Pending,
    Assigned,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl TaskAttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

#[cfg(test)]
mod tests {
    use super::{JobState, StageState, TaskAttemptState};

    #[test]
    fn terminal_state_helpers_match_design() {
        assert!(JobState::Succeeded.is_terminal());
        assert!(StageState::Failed.is_terminal());
        assert!(TaskAttemptState::Canceled.is_terminal());
        assert!(!JobState::Running.is_terminal());
    }
}
