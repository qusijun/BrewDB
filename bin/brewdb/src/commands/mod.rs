//! Top-level CLI command handlers.

use brewdb_core::common::RequestContext;
use brewdb_core::execution::BoundaryKind;
use brewdb_core::ids::{JobId, RequestId, StageId, TaskId};
use brewdb_execution::plan::{StageGraph, StageKind, StagePlan, TaskPlan};
use brewdb_runtime::admission::{
    AdmissionBootstrap, AdmissionService, AdmitRequest, DirectAdmissionService,
};
use brewdb_runtime::planning::{BuildPlan, PlanSpec};
use brewdb_sql::intent::{FrontendSqlRequest, IntentPlanner, QueryOnlyIntentPlanner, SqlIntent};

/// Minimal query request entering the phase-1 CLI loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinimalQueryRequest {
    pub sql: String,
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
    pub request_context: RequestContext,
}

/// Bootstrap state returned once the query path enters runtime ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinimalQueryBootstrap {
    pub job_id: JobId,
    pub sql_intent: SqlIntent,
    pub admission: AdmissionBootstrap,
}

/// Build the first request-entry to admission bootstrap for the query loop.
pub fn bootstrap_minimal_query(
    request: MinimalQueryRequest,
) -> Result<MinimalQueryBootstrap, MinimalQueryBootstrapError> {
    let planner = QueryOnlyIntentPlanner;
    let sql_result = planner.build_intent(FrontendSqlRequest {
        sql: request.sql,
        request_context: request.request_context.clone(),
        default_catalog: request.default_catalog,
        default_database: request.default_database,
    })?;

    let job_id = JobId::generate();
    let build_plan = BuildPlan {
        job_id: job_id.clone(),
        request_context: request.request_context,
        spec: PlanSpec::Query {
            stage_graph: query_stage_graph(job_id.clone()),
        },
    };

    let admission = DirectAdmissionService.admit_request(AdmitRequest { build_plan })?;

    Ok(MinimalQueryBootstrap {
        job_id,
        sql_intent: sql_result.intent,
        admission,
    })
}

fn query_stage_graph(job_id: JobId) -> StageGraph {
    let stage_id = StageId::generate();
    let task_id = TaskId::generate();

    StageGraph::new(
        job_id.clone(),
        vec![StagePlan {
            stage_id: stage_id.clone(),
            kind: StageKind::Compute,
            boundary: Some(BoundaryKind::Exchange),
        }],
    )
    .with_tasks(vec![TaskPlan {
        task_id,
        stage_id,
        partition_id: 0,
        dependencies: Vec::new(),
    }])
}

#[derive(Debug)]
pub enum MinimalQueryBootstrapError {
    Sql(brewdb_sql::errors::SqlError),
    Runtime(brewdb_runtime::errors::RuntimeError),
}

impl std::fmt::Display for MinimalQueryBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(error) => write!(f, "{error}"),
            Self::Runtime(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for MinimalQueryBootstrapError {}

impl From<brewdb_sql::errors::SqlError> for MinimalQueryBootstrapError {
    fn from(value: brewdb_sql::errors::SqlError) -> Self {
        Self::Sql(value)
    }
}

impl From<brewdb_runtime::errors::RuntimeError> for MinimalQueryBootstrapError {
    fn from(value: brewdb_runtime::errors::RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

pub fn sample_query_request() -> MinimalQueryRequest {
    MinimalQueryRequest {
        sql: "select 1".to_owned(),
        default_catalog: Some("brew".to_owned()),
        default_database: Some("default".to_owned()),
        request_context: RequestContext::new().with_request_id(RequestId::generate()),
    }
}

#[cfg(test)]
mod tests {
    use super::{bootstrap_minimal_query, sample_query_request};

    #[test]
    fn query_bootstrap_enters_runtime_with_stage_graph() {
        let bootstrap = bootstrap_minimal_query(sample_query_request()).unwrap();

        assert_eq!(
            bootstrap.admission.orchestration_plan.job_id,
            bootstrap.job_id
        );
        assert_eq!(bootstrap.admission.stage_ids().len(), 1);
    }
}
