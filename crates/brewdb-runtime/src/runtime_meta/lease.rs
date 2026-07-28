//! Runtime coordination and lease records.

use brewdb_core::ids::{JobId, TableId};
use brewdb_core::txn::ResourceLane;

/// Runtime truth for the active owner of one job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobOwnerRecord {
    pub job_id: JobId,
    pub owner_id: String,
}

impl JobOwnerRecord {
    pub fn new(job_id: JobId, owner_id: impl Into<String>) -> Self {
        Self {
            job_id,
            owner_id: owner_id.into(),
        }
    }
}

/// Runtime truth for one table-lane lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceLeaseRecord {
    pub table_id: TableId,
    pub lane: ResourceLane,
    pub holder_id: String,
}

impl ResourceLeaseRecord {
    pub fn new(table_id: TableId, lane: ResourceLane, holder_id: impl Into<String>) -> Self {
        Self {
            table_id,
            lane,
            holder_id: holder_id.into(),
        }
    }
}

/// Runtime truth for one cluster-scoped housekeeping lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterLeaseRecord {
    pub lease_name: String,
    pub holder_id: String,
}

impl ClusterLeaseRecord {
    pub fn new(lease_name: impl Into<String>, holder_id: impl Into<String>) -> Self {
        Self {
            lease_name: lease_name.into(),
            holder_id: holder_id.into(),
        }
    }
}
