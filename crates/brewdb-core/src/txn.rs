//! Shared transaction and commit state types.

use crate::ids::{CommitAttemptId, TxnId};

/// Shared table-level critical-section lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceLane {
    Ddl,
    Mutation,
    Maintenance,
}

/// Runtime transaction state shared across crates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TxnState {
    Open,
    Validating,
    Committing,
    Committed,
    Aborting,
    Aborted,
    UnknownOutcome,
}

impl TxnState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Open, Self::Validating)
                | (Self::Open, Self::Aborting)
                | (Self::Validating, Self::Committing)
                | (Self::Validating, Self::Aborting)
                | (Self::Committing, Self::Committed)
                | (Self::Committing, Self::UnknownOutcome)
                | (Self::Committing, Self::Aborting)
                | (Self::Aborting, Self::Aborted)
                | (Self::UnknownOutcome, Self::Committed)
                | (Self::UnknownOutcome, Self::Aborted)
        )
    }
}

/// One concrete validate/publish attempt under a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommitAttemptState {
    Created,
    Validating,
    Publishing,
    Succeeded,
    Failed,
    UnknownOutcome,
}

impl CommitAttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::UnknownOutcome)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Created, Self::Validating)
                | (Self::Validating, Self::Publishing)
                | (Self::Validating, Self::Failed)
                | (Self::Publishing, Self::Succeeded)
                | (Self::Publishing, Self::Failed)
                | (Self::Publishing, Self::UnknownOutcome)
        )
    }
}

/// The role currently holding transaction-management authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TxnLockHolderKind {
    Owner,
    Retry,
    Reconciler,
}

/// Shared transaction-lock record shape.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TxnLockRecord {
    pub txn_id: TxnId,
    pub holder_kind: TxnLockHolderKind,
    pub holder_id: String,
    pub fencing_epoch: u64,
    pub acquired_at_ms: u64,
    pub expires_at_ms: u64,
    pub heartbeat_at_ms: u64,
}

impl TxnLockRecord {
    pub fn is_expired_at(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

/// Shared commit-attempt summary shell.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CommitAttemptRef {
    pub txn_id: TxnId,
    pub commit_attempt_id: CommitAttemptId,
    pub state: CommitAttemptState,
}

#[cfg(test)]
mod tests {
    use super::{
        CommitAttemptRef, CommitAttemptState, ResourceLane, TxnLockHolderKind, TxnLockRecord,
        TxnState,
    };
    use crate::ids::{CommitAttemptId, TxnId};

    #[test]
    fn terminal_helpers_match_runtime_rules() {
        assert!(TxnState::Committed.is_terminal());
        assert!(TxnState::Aborted.is_terminal());
        assert!(!TxnState::UnknownOutcome.is_terminal());
        assert!(CommitAttemptState::Failed.is_terminal());
        assert!(!CommitAttemptState::Publishing.is_terminal());
        assert!(TxnState::Open.can_transition_to(TxnState::Validating));
        assert!(!TxnState::Open.can_transition_to(TxnState::Committed));
        assert!(CommitAttemptState::Created.can_transition_to(CommitAttemptState::Validating));
        assert!(!CommitAttemptState::Created.can_transition_to(CommitAttemptState::Publishing));
    }

    #[test]
    fn txn_lock_record_carries_fencing_shape() {
        let lock = TxnLockRecord {
            txn_id: TxnId::parse_str("550e8400-e29b-41d4-a716-446655440010").unwrap(),
            holder_kind: TxnLockHolderKind::Owner,
            holder_id: "coordinator-a".to_owned(),
            fencing_epoch: 3,
            acquired_at_ms: 10,
            expires_at_ms: 20,
            heartbeat_at_ms: 15,
        };

        let attempt = CommitAttemptRef {
            txn_id: lock.txn_id.clone(),
            commit_attempt_id: CommitAttemptId::parse_str("550e8400-e29b-41d4-a716-446655440011")
                .unwrap(),
            state: CommitAttemptState::Created,
        };

        assert_eq!(lock.holder_kind, TxnLockHolderKind::Owner);
        assert_eq!(attempt.state, CommitAttemptState::Created);
        assert_eq!(ResourceLane::Mutation as u8, ResourceLane::Mutation as u8);
        assert!(!lock.is_expired_at(19));
        assert!(lock.is_expired_at(20));
    }
}
