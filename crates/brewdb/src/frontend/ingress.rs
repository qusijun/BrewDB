//! Frontend-facing SQL ingress boundary.

use crate::common::context::SessionContext;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlClientCapabilities {
    pub supports_prepared_statements: bool,
    pub supports_portals: bool,
    pub supports_streaming_results: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlRequestContext {
    pub request_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressSql {
    pub session: SessionContext,
    pub request: SqlRequestContext,
    pub sql: String,
    pub client_capabilities: Option<SqlClientCapabilities>,
}
