//! Transaction runtime truth records.

use brewdb_core::ids::{CommitAttemptId, JobId, TxnId};
use brewdb_core::txn::TxnState;

/// Persisted transaction truth for one commit-bearing job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnRecord {
    pub txn_id: TxnId,
    pub job_id: JobId,
    pub state: TxnState,
    pub active_commit_attempt_id: Option<CommitAttemptId>,
}

impl TxnRecord {
    pub fn new(txn_id: TxnId, job_id: JobId) -> Self {
        Self {
            txn_id,
            job_id,
            state: TxnState::Open,
            active_commit_attempt_id: None,
        }
    }

    pub fn with_active_commit_attempt(mut self, commit_attempt_id: CommitAttemptId) -> Self {
        self.active_commit_attempt_id = Some(commit_attempt_id);
        self
    }
}
