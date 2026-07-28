//! Transaction orchestration aggregates.

use brewdb_core::txn::TxnLockRecord;

use crate::runtime_meta::{CommitAttemptRecord, TxnRecord};

/// Runtime aggregate for one transaction-finalization workflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnContext {
    pub txn: TxnRecord,
    pub active_commit_attempt: Option<CommitAttemptRecord>,
    pub txn_lock: Option<TxnLockRecord>,
}

impl TxnContext {
    pub fn new(txn: TxnRecord) -> Self {
        Self {
            txn,
            active_commit_attempt: None,
            txn_lock: None,
        }
    }

    pub fn with_active_commit_attempt(mut self, attempt: CommitAttemptRecord) -> Self {
        self.active_commit_attempt = Some(attempt);
        self
    }

    pub fn with_txn_lock(mut self, txn_lock: TxnLockRecord) -> Self {
        self.txn_lock = Some(txn_lock);
        self
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::{CommitAttemptId, JobId, TxnId};
    use brewdb_core::txn::{TxnLockHolderKind, TxnLockRecord};

    use super::TxnContext;
    use crate::runtime_meta::{CommitAttemptRecord, TxnRecord};

    #[test]
    fn txn_context_assembles_runtime_aggregate() {
        let txn = TxnRecord::new(
            TxnId::parse_str("550e8400-e29b-41d4-a716-446655440100").unwrap(),
            JobId::parse_str("550e8400-e29b-41d4-a716-446655440101").unwrap(),
        );
        let attempt = CommitAttemptRecord::new(
            CommitAttemptId::parse_str("550e8400-e29b-41d4-a716-446655440102").unwrap(),
            txn.txn_id.clone(),
            1,
        );
        let txn_lock = TxnLockRecord {
            txn_id: txn.txn_id.clone(),
            holder_kind: TxnLockHolderKind::Owner,
            holder_id: "owner-a".to_owned(),
            fencing_epoch: 1,
            acquired_at_ms: 10,
            expires_at_ms: 20,
            heartbeat_at_ms: 15,
        };

        let context = TxnContext::new(txn.clone())
            .with_active_commit_attempt(attempt.clone())
            .with_txn_lock(txn_lock.clone());

        assert_eq!(context.txn, txn);
        assert_eq!(context.active_commit_attempt, Some(attempt));
        assert_eq!(context.txn_lock, Some(txn_lock));
    }
}
