//! Commit orchestration and attempt management.

use brewdb_core::ids::{CommitAttemptId, TxnId};

use crate::errors::RuntimeError;
use crate::runtime_meta::CommitAttemptRecord;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCommitAttempt {
    pub commit_attempt_id: CommitAttemptId,
    pub txn_id: TxnId,
    pub publish_epoch: u64,
}

impl CreateCommitAttempt {
    pub fn into_record(self) -> CommitAttemptRecord {
        CommitAttemptRecord::new(self.commit_attempt_id, self.txn_id, self.publish_epoch)
    }
}

pub trait CommitService {
    fn create_commit_attempt(
        &self,
        command: CreateCommitAttempt,
    ) -> Result<CommitAttemptRecord, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{CommitAttemptId, TxnId};

    use super::CreateCommitAttempt;

    #[test]
    fn create_commit_attempt_builds_record() {
        let command = CreateCommitAttempt {
            commit_attempt_id: CommitAttemptId::parse_str("550e8400-e29b-41d4-a716-446655440230")
                .unwrap(),
            txn_id: TxnId::parse_str("550e8400-e29b-41d4-a716-446655440231").unwrap(),
            publish_epoch: 7,
        };

        let record = command.into_record();

        assert_eq!(record.publish_epoch, 7);
        assert_eq!(
            record.txn_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440231"
        );
    }
}
