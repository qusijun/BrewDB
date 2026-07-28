//! Commit-attempt runtime truth records.

use brewdb_core::ids::{CommitAttemptId, TxnId};
use brewdb_core::txn::CommitAttemptState;

/// Persisted runtime truth for one validate/publish attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitAttemptRecord {
    pub commit_attempt_id: CommitAttemptId,
    pub txn_id: TxnId,
    pub state: CommitAttemptState,
    pub publish_epoch: u64,
}

impl CommitAttemptRecord {
    pub fn new(commit_attempt_id: CommitAttemptId, txn_id: TxnId, publish_epoch: u64) -> Self {
        Self {
            commit_attempt_id,
            txn_id,
            state: CommitAttemptState::Created,
            publish_epoch,
        }
    }
}
