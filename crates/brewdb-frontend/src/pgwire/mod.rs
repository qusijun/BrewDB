//! PostgreSQL wire protocol shell.

use brewdb_core::common::RequestContext;
use brewdb_core::ids::JobId;
use brewdb_runtime::admission::AdmissionBootstrap;
use brewdb_runtime::planning::{BuildPlan, PlanSpec};
use brewdb_sql::intent::{FrontendSqlRequest, IntentPlanner, QueryOnlyIntentPlanner};

use crate::auth::{AllowAllAuthenticator, AuthRequest, Authenticator};
use crate::errors::FrontendError;
use crate::result::QueryResultEnvelope;
use crate::session::{DirectSessionService, FrontendSession, OpenSession, SessionService};

/// SQL query request entering the pgwire adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgwireQueryRequest {
    pub sql: String,
    pub request_context: RequestContext,
    pub user_name: Option<String>,
    pub database_name: Option<String>,
}

/// Output of the pgwire ingress shell before distributed execution begins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgwireQueryBootstrap {
    pub session: FrontendSession,
    pub admission: AdmissionBootstrap,
}

/// Minimal pgwire-facing response shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PgwireResponse {
    QueryAccepted(QueryResultEnvelope),
}

/// Frontend pgwire service boundary.
pub trait PgwireService {
    fn bootstrap_query(
        &self,
        request: PgwireQueryRequest,
    ) -> Result<PgwireQueryBootstrap, FrontendError>;
}

/// Phase 1 pgwire service shell.
#[derive(Clone, Debug, Default)]
pub struct DirectPgwireService;

impl PgwireService for DirectPgwireService {
    fn bootstrap_query(
        &self,
        request: PgwireQueryRequest,
    ) -> Result<PgwireQueryBootstrap, FrontendError> {
        if request.sql.trim().is_empty() {
            return Err(FrontendError::MissingField {
                entity: "pgwire_query_request",
                field: "sql",
            });
        }

        let auth_context = AllowAllAuthenticator.authenticate(AuthRequest {
            user_name: request.user_name.clone(),
            database_name: request.database_name.clone(),
        })?;
        let session = DirectSessionService.open_session(OpenSession {
            request_context: request.request_context.clone(),
            auth_context,
        })?;

        let planner = QueryOnlyIntentPlanner;
        planner
            .build_intent(FrontendSqlRequest {
                sql: request.sql,
                request_context: request.request_context.clone(),
                default_catalog: None,
                default_database: request.database_name,
            })
            .map_err(|error| FrontendError::Unsupported {
                operation: "sql_intent",
                reason: error.to_string(),
            })?;

        let job_id = JobId::generate();
        let build_plan = BuildPlan {
            job_id: job_id.clone(),
            request_context: request.request_context,
            spec: PlanSpec::Query {
                stage_graph: query_stage_graph(job_id),
            },
        };
        let admission = build_plan.bootstrap_query()?;

        Ok(PgwireQueryBootstrap { session, admission })
    }
}

fn query_stage_graph(job_id: JobId) -> brewdb_execution::plan::StageGraph {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{StageId, TaskId};
    use brewdb_execution::plan::{StageGraph, StageKind, StagePlan, TaskPlan};

    let stage_id = StageId::generate();

    StageGraph::new(
        job_id,
        vec![StagePlan {
            stage_id: stage_id.clone(),
            kind: StageKind::Compute,
            boundary: Some(BoundaryKind::Exchange),
        }],
    )
    .with_tasks(vec![TaskPlan {
        task_id: TaskId::generate(),
        stage_id,
        partition_id: 0,
        dependencies: Vec::new(),
    }])
}

trait BuildPlanBootstrapExt {
    fn bootstrap_query(self) -> Result<AdmissionBootstrap, FrontendError>;
}

impl BuildPlanBootstrapExt for BuildPlan {
    fn bootstrap_query(self) -> Result<AdmissionBootstrap, FrontendError> {
        use brewdb_runtime::admission::{AdmissionService, AdmitRequest, DirectAdmissionService};

        DirectAdmissionService
            .admit_request(AdmitRequest { build_plan: self })
            .map_err(|error| FrontendError::Unsupported {
                operation: "runtime_admission",
                reason: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::common::RequestContext;

    use super::{DirectPgwireService, PgwireQueryRequest, PgwireService};

    #[test]
    fn pgwire_bootstrap_opens_session_and_admits_query() {
        let bootstrap = DirectPgwireService
            .bootstrap_query(PgwireQueryRequest {
                sql: "select 1".to_owned(),
                request_context: RequestContext::new(),
                user_name: Some("brew".to_owned()),
                database_name: Some("default".to_owned()),
            })
            .unwrap();

        assert!(bootstrap.session.auth_context.authenticated);
        assert_eq!(bootstrap.admission.stage_ids().len(), 1);
    }
}
