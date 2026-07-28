//! Execution-complete finalization shell.

use brewdb_core::artifacts::ArtifactBundleRef;
use brewdb_core::catalog::TableRef;
use brewdb_core::ids::{CommitAttemptId, JobId, TxnId};

use crate::commit::CreateCommitAttempt;
use crate::errors::RuntimeError;
use crate::locks::AcquireTxnLock;
use crate::runtime_meta::{CommitAttemptRecord, TxnRecord};
use crate::txns::OpenTxn;

/// Finalization request after execution has produced result artifacts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizeExecution {
    pub job_id: JobId,
    pub target_table: Option<TableRef>,
    pub artifact_bundle: Option<ArtifactBundleRef>,
    pub requires_txn: bool,
}

/// Commit-path bootstrap shell emitted when a job enters txn-bearing finalization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPathBootstrap {
    pub open_txn: Option<OpenTxn>,
    pub acquire_txn_lock: Option<AcquireTxnLock>,
    pub create_commit_attempt: Option<CreateCommitAttempt>,
}

/// Abort-path shell emitted when a job finalizes as aborted/canceled/failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortPath {
    pub job_id: JobId,
    pub txn_id: Option<TxnId>,
    pub reason: String,
}

/// Reconciliation entry shell for unknown-outcome convergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileFinalization {
    pub job_id: JobId,
    pub txn_id: TxnId,
    pub active_commit_attempt_id: Option<CommitAttemptId>,
}

/// Finalization outcome shell visible to runtime orchestration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizationPlan {
    pub job_id: JobId,
    pub commit_path: Option<CommitPathBootstrap>,
    pub abort_path: Option<AbortPath>,
    pub reconcile: Option<ReconcileFinalization>,
}

/// Finalization boundary over commit, abort, and reconcile entry paths.
pub trait FinalizationService {
    fn finalize_execution(
        &self,
        command: FinalizeExecution,
    ) -> Result<FinalizationPlan, RuntimeError>;

    fn bootstrap_commit_path(
        &self,
        txn: TxnRecord,
        attempt: CommitAttemptRecord,
    ) -> Result<CommitPathBootstrap, RuntimeError>;

    fn abort_path(&self, command: AbortPath) -> Result<AbortPath, RuntimeError>;

    fn reconcile_entry(
        &self,
        command: ReconcileFinalization,
    ) -> Result<ReconcileFinalization, RuntimeError>;
}

impl FinalizeExecution {
    pub fn into_open_txn(&self, txn_id: TxnId) -> Option<OpenTxn> {
        self.requires_txn.then(|| OpenTxn {
            txn_id,
            job_id: self.job_id.clone(),
        })
    }
}

impl CommitPathBootstrap {
    pub fn new(
        open_txn: Option<OpenTxn>,
        acquire_txn_lock: Option<AcquireTxnLock>,
        create_commit_attempt: Option<CreateCommitAttempt>,
    ) -> Self {
        Self {
            open_txn,
            acquire_txn_lock,
            create_commit_attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::artifacts::{ArtifactBundleKind, ArtifactBundleRef, ArtifactRef};
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{
        ArtifactId, CommitAttemptId, JobId, NamespaceId, TableId, TxnId, WarehouseId,
    };
    use brewdb_core::txn::TxnLockHolderKind;

    use crate::commit::CreateCommitAttempt;
    use crate::locks::AcquireTxnLock;
    use crate::txns::OpenTxn;

    use super::{AbortPath, CommitPathBootstrap, FinalizationPlan, FinalizeExecution};

    fn table_ref() -> TableRef {
        TableRef {
            namespace_id: NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441000").unwrap(),
            table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655441001").unwrap(),
            warehouse_id: WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441002").unwrap(),
            format_type: FormatType::Iceberg,
        }
    }

    #[test]
    fn finalize_execution_can_bootstrap_commit_path() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655441003").unwrap();
        let command = FinalizeExecution {
            job_id: job_id.clone(),
            target_table: Some(table_ref()),
            artifact_bundle: Some(ArtifactBundleRef {
                job_id: job_id.clone(),
                txn_id: None,
                kind: ArtifactBundleKind::Append,
                artifacts: vec![ArtifactRef {
                    artifact_id: ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655441004")
                        .unwrap(),
                    location: "s3://warehouse/tmp/a.parquet".to_owned(),
                }],
            }),
            requires_txn: true,
        };

        let open_txn = command
            .into_open_txn(TxnId::parse_str("550e8400-e29b-41d4-a716-446655441005").unwrap());

        assert!(open_txn.is_some());
    }

    #[test]
    fn commit_path_bootstrap_keeps_handoff_commands() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655441010").unwrap();
        let txn_id = TxnId::parse_str("550e8400-e29b-41d4-a716-446655441011").unwrap();
        let bootstrap = CommitPathBootstrap::new(
            Some(OpenTxn {
                txn_id: txn_id.clone(),
                job_id,
            }),
            Some(AcquireTxnLock {
                txn_id: txn_id.clone(),
                holder_kind: TxnLockHolderKind::Owner,
                holder_id: "owner-a".to_owned(),
                fencing_epoch: 1,
                acquired_at_ms: 10,
                expires_at_ms: 20,
                heartbeat_at_ms: 15,
            }),
            Some(CreateCommitAttempt {
                commit_attempt_id: CommitAttemptId::parse_str(
                    "550e8400-e29b-41d4-a716-446655441012",
                )
                .unwrap(),
                txn_id,
                publish_epoch: 7,
            }),
        );

        assert!(bootstrap.open_txn.is_some());
        assert!(bootstrap.acquire_txn_lock.is_some());
        assert!(bootstrap.create_commit_attempt.is_some());
    }

    #[test]
    fn finalization_plan_can_expose_abort_path() {
        let abort_path = AbortPath {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655441020").unwrap(),
            txn_id: None,
            reason: "user canceled".to_owned(),
        };
        let plan = FinalizationPlan {
            job_id: abort_path.job_id.clone(),
            commit_path: None,
            abort_path: Some(abort_path.clone()),
            reconcile: None,
        };

        assert_eq!(plan.abort_path, Some(abort_path));
    }
}
