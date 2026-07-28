//! Commit validation, publish, and reconciliation contracts.

use brewdb_core::artifacts::ArtifactBundleRef;
use brewdb_core::catalog::TableRef;

use crate::errors::StorageError;

/// Commit preparation request assembled after execution has produced staged artifacts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrepareCommit {
    pub table: TableRef,
    pub artifact_bundle: ArtifactBundleRef,
    pub validate_conflicts: bool,
}

/// Commit preparation output handed back to runtime finalization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPreparation {
    pub table: TableRef,
    pub artifact_bundle: ArtifactBundleRef,
    pub validation_summary: Option<String>,
    pub publish_token: Option<String>,
}

/// Storage commit preparation boundary.
pub trait CommitPreparationService {
    fn prepare_commit(&self, request: PrepareCommit) -> Result<CommitPreparation, StorageError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::artifacts::{ArtifactBundleKind, ArtifactBundleRef, ArtifactRef};
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{ArtifactId, JobId, NamespaceId, TableId, WarehouseId};

    use super::{CommitPreparation, PrepareCommit};

    fn table_ref() -> TableRef {
        TableRef {
            namespace_id: NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655440940").unwrap(),
            table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655440941").unwrap(),
            warehouse_id: WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655440942").unwrap(),
            format_type: FormatType::Iceberg,
        }
    }

    #[test]
    fn commit_preparation_shell_carries_bundle_and_publish_token() {
        let bundle = ArtifactBundleRef {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440943").unwrap(),
            txn_id: None,
            kind: ArtifactBundleKind::Append,
            artifacts: vec![ArtifactRef {
                artifact_id: ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655440944").unwrap(),
                location: "s3://warehouse/tmp/a.parquet".to_owned(),
            }],
        };
        let request = PrepareCommit {
            table: table_ref(),
            artifact_bundle: bundle.clone(),
            validate_conflicts: true,
        };
        let preparation = CommitPreparation {
            table: request.table.clone(),
            artifact_bundle: bundle,
            validation_summary: Some("validated".to_owned()),
            publish_token: Some("publish-1".to_owned()),
        };

        assert!(request.validate_conflicts);
        assert_eq!(preparation.publish_token.as_deref(), Some("publish-1"));
    }
}
