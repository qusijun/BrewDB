//! Job orchestration aggregates.

use crate::contexts::TxnContext;
use crate::runtime_meta::{JobOwnerRecord, JobRecord, ResourceLeaseRecord};

/// Runtime aggregate for one job orchestration workflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobContext {
    pub job: JobRecord,
    pub owner: Option<JobOwnerRecord>,
    pub resource_lease: Option<ResourceLeaseRecord>,
    pub txn: Option<TxnContext>,
}

impl JobContext {
    pub fn new(job: JobRecord) -> Self {
        Self {
            job,
            owner: None,
            resource_lease: None,
            txn: None,
        }
    }

    pub fn with_owner(mut self, owner: JobOwnerRecord) -> Self {
        self.owner = Some(owner);
        self
    }

    pub fn with_resource_lease(mut self, resource_lease: ResourceLeaseRecord) -> Self {
        self.resource_lease = Some(resource_lease);
        self
    }

    pub fn with_txn(mut self, txn: TxnContext) -> Self {
        self.txn = Some(txn);
        self
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::common::RequestContext;
    use brewdb_core::ids::{JobId, TableId};
    use brewdb_core::txn::ResourceLane;

    use super::JobContext;
    use crate::runtime_meta::{JobOwnerRecord, JobRecord, ResourceLeaseRecord};

    #[test]
    fn job_context_assembles_runtime_aggregate() {
        let job = JobRecord::new(
            JobId::parse_str("550e8400-e29b-41d4-a716-446655440110").unwrap(),
            RequestContext::new(),
        )
        .with_target_table(TableId::parse_str("550e8400-e29b-41d4-a716-446655440111").unwrap())
        .with_lane(ResourceLane::Mutation);
        let owner = JobOwnerRecord::new(job.job_id.clone(), "owner-a");
        let lease = ResourceLeaseRecord::new(
            job.target_table_id.clone().unwrap(),
            ResourceLane::Mutation,
            "owner-a",
        );

        let context = JobContext::new(job.clone())
            .with_owner(owner.clone())
            .with_resource_lease(lease.clone());

        assert_eq!(context.job, job);
        assert_eq!(context.owner, Some(owner));
        assert_eq!(context.resource_lease, Some(lease));
        assert!(context.txn.is_none());
    }
}
