//! Frontend-facing SQL ingress boundary.

use uuid::Uuid;

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
    pub client_capabilities: Option<SqlClientCapabilities>,
}
