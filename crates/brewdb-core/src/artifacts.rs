//! Shared artifact identifiers and record shells.

use crate::ids::{ArtifactId, JobId, TxnId};

/// A lightweight shared view of one staged artifact.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactRef {
    pub artifact_id: ArtifactId,
    pub location: String,
}

/// Shared artifact bundle kinds used by runtime and storage layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArtifactBundleKind {
    Append,
    Rewrite,
    Maintenance,
    Selection,
}

/// A lightweight shared bundle shell for commit-oriented artifact grouping.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArtifactBundleRef {
    pub job_id: JobId,
    pub txn_id: Option<TxnId>,
    pub kind: ArtifactBundleKind,
    pub artifacts: Vec<ArtifactRef>,
}
