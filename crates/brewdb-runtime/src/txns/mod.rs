//! Transaction lifecycle orchestration.

use brewdb_core::ids::{CommitAttemptId, JobId, TxnId};
use brewdb_core::txn::TxnState;

use crate::errors::RuntimeError;
use crate::runtime_meta::TxnRecord;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenTxn {
    pub txn_id: TxnId,
    pub job_id: JobId,
}

impl OpenTxn {
    pub fn into_record(self) -> TxnRecord {
        TxnRecord::new(self.txn_id, self.job_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateTxnState {
    pub txn_id: TxnId,
    pub next_state: TxnState,
    pub active_commit_attempt_id: Option<CommitAttemptId>,
}

pub trait TxnService {
    fn open_txn(&self, command: OpenTxn) -> Result<TxnRecord, RuntimeError>;
    fn update_txn_state(&self, command: UpdateTxnState) -> Result<TxnRecord, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{JobId, TxnId};

    use super::OpenTxn;

    #[test]
    fn open_txn_builds_txn_record() {
        let command = OpenTxn {
            txn_id: TxnId::parse_str("550e8400-e29b-41d4-a716-446655440210").unwrap(),
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440211").unwrap(),
        };

        let record = command.into_record();

        assert_eq!(
            record.txn_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440210"
        );
        assert_eq!(
            record.job_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440211"
        );
    }
}
