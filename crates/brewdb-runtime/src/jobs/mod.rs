//! Job lifecycle orchestration.

use brewdb_core::common::RequestContext;
use brewdb_core::ids::{JobId, TableId};
use brewdb_core::state::JobState;
use brewdb_core::txn::ResourceLane;

use crate::errors::RuntimeError;
use crate::runtime_meta::JobRecord;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitJob {
    pub job_id: JobId,
    pub target_table_id: Option<TableId>,
    pub lane: Option<ResourceLane>,
    pub request_context: RequestContext,
}

impl SubmitJob {
    pub fn into_record(self) -> JobRecord {
        let mut record = JobRecord::new(self.job_id, self.request_context);

        if let Some(table_id) = self.target_table_id {
            record = record.with_target_table(table_id);
        }

        if let Some(lane) = self.lane {
            record = record.with_lane(lane);
        }

        record
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateJobState {
    pub job_id: JobId,
    pub next_state: JobState,
}

pub trait JobService {
    fn submit_job(&self, command: SubmitJob) -> Result<JobRecord, RuntimeError>;
    fn update_job_state(&self, command: UpdateJobState) -> Result<JobRecord, RuntimeError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::JobId;
    use brewdb_core::txn::ResourceLane;

    use super::SubmitJob;

    #[test]
    fn submit_job_builds_job_record() {
        let command = SubmitJob {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440200").unwrap(),
            target_table_id: None,
            lane: Some(ResourceLane::Mutation),
            request_context: Default::default(),
        };

        let record = command.into_record();

        assert_eq!(
            record.job_id.to_string(),
            "550e8400-e29b-41d4-a716-446655440200"
        );
        assert_eq!(record.lane, Some(ResourceLane::Mutation));
    }
}
