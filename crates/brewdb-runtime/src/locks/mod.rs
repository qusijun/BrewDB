//! Transaction-management locks and fencing control.

use brewdb_core::ids::TxnId;
use brewdb_core::txn::{TxnLockHolderKind, TxnLockRecord};

use crate::errors::RuntimeError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcquireTxnLock {
    pub txn_id: TxnId,
    pub holder_kind: TxnLockHolderKind,
    pub holder_id: String,
    pub fencing_epoch: u64,
    pub acquired_at_ms: u64,
    pub expires_at_ms: u64,
    pub heartbeat_at_ms: u64,
}

impl AcquireTxnLock {
    pub fn into_record(self) -> TxnLockRecord {
        TxnLockRecord {
            txn_id: self.txn_id,
            holder_kind: self.holder_kind,
            holder_id: self.holder_id,
            fencing_epoch: self.fencing_epoch,
            acquired_at_ms: self.acquired_at_ms,
            expires_at_ms: self.expires_at_ms,
            heartbeat_at_ms: self.heartbeat_at_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatTxnLock {
    pub txn_id: TxnId,
    pub expected_fencing_epoch: u64,
    pub heartbeat_at_ms: u64,
    pub expires_at_ms: u64,
}

pub trait TxnLockService {
    fn acquire_txn_lock(&self, command: AcquireTxnLock) -> Result<TxnLockRecord, RuntimeError>;
    fn heartbeat_txn_lock(&self, command: HeartbeatTxnLock) -> Result<TxnLockRecord, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::TxnId;
    use brewdb_core::txn::TxnLockHolderKind;

    use super::AcquireTxnLock;

    #[test]
    fn acquire_lock_builds_lock_record() {
        let command = AcquireTxnLock {
            txn_id: TxnId::parse_str("550e8400-e29b-41d4-a716-446655440220").unwrap(),
            holder_kind: TxnLockHolderKind::Owner,
            holder_id: "owner-a".to_owned(),
            fencing_epoch: 2,
            acquired_at_ms: 100,
            expires_at_ms: 200,
            heartbeat_at_ms: 150,
        };

        let record = command.into_record();

        assert_eq!(record.fencing_epoch, 2);
        assert_eq!(record.holder_id, "owner-a");
    }
}
