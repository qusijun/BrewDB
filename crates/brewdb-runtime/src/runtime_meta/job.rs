//! Job runtime truth records.

use brewdb_core::common::RequestContext;
use brewdb_core::ids::{JobId, StageId, TableId};
use brewdb_core::state::{JobState, StageState};
use brewdb_core::txn::ResourceLane;

/// Persisted lifecycle truth for one admitted BrewDB job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRecord {
    pub job_id: JobId,
    pub state: JobState,
    pub target_table_id: Option<TableId>,
    pub lane: Option<ResourceLane>,
    pub request_context: RequestContext,
}

impl JobRecord {
    pub fn new(job_id: JobId, request_context: RequestContext) -> Self {
        Self {
            job_id,
            state: JobState::Pending,
            target_table_id: None,
            lane: None,
            request_context,
        }
    }

    pub fn with_target_table(mut self, table_id: TableId) -> Self {
        self.target_table_id = Some(table_id);
        self
    }

    pub fn with_lane(mut self, lane: ResourceLane) -> Self {
        self.lane = Some(lane);
        self
    }
}

/// Persisted lifecycle truth for one execution stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageRecord {
    pub stage_id: StageId,
    pub job_id: JobId,
    pub state: StageState,
}

impl StageRecord {
    pub fn new(stage_id: StageId, job_id: JobId) -> Self {
        Self {
            stage_id,
            job_id,
            state: StageState::Pending,
        }
    }
}
