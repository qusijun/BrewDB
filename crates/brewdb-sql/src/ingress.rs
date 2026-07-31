//! Frontend-facing SQL ingress boundary.

use uuid::Uuid;

use crate::errors::SqlError;
use crate::statement::{
    RuntimeStatement, SessionStatement, SqlStatementEnvelope, StatementCategory, StatementPayload,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontendStatementRouteScope {
    SessionLocal,
    RuntimeBound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendStatementRoute {
    pub scope: FrontendStatementRouteScope,
    pub statement_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlClientCapabilities {
    pub supports_prepared_statements: bool,
    pub supports_portals: bool,
    pub supports_streaming_results: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlSessionContext {
    pub session_id: Uuid,
    pub user_name: String,
    pub database_name: Option<String>,
    pub catalog_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlRequestContext {
    pub request_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlIngressRequest {
    pub session: SqlSessionContext,
    pub request: SqlRequestContext,
    pub sql: String,
    pub route: FrontendStatementRoute,
    pub client_capabilities: Option<SqlClientCapabilities>,
}

#[derive(Clone, Debug, Default)]
pub struct SqlDriver;

impl SqlDriver {
    pub fn analyze(&self, request: SqlIngressRequest) -> Result<SqlStatementEnvelope, SqlError> {
        let statement_text = request.sql.trim().to_string();
        if statement_text.is_empty() {
            return Err(SqlError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        let (category, payload) = match request.route.scope {
            FrontendStatementRouteScope::SessionLocal => (
                StatementCategory::Session,
                StatementPayload::Session(SessionStatement),
            ),
            FrontendStatementRouteScope::RuntimeBound => (
                StatementCategory::Runtime,
                StatementPayload::Runtime(RuntimeStatement),
            ),
        };

        Ok(SqlStatementEnvelope {
            statement_text,
            statement_name: request.route.statement_name,
            category,
            route_scope: request.route.scope,
            payload,
        })
    }
}
