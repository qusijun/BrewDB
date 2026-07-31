//! Protocol-neutral session and request handling.

use brewdb_sql::{
    FrontendStatementRoute, FrontendStatementRouteScope, SqlClientCapabilities, SqlIngressRequest,
    SqlRequestContext, SqlSessionContext,
};
use uuid::Uuid;

use crate::auth::{AuthContext, Authenticator};
use crate::errors::FrontendError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientIdentity {
    pub user_name: String,
    pub database_name: Option<String>,
}

impl ClientIdentity {
    pub fn new(user_name: impl Into<String>) -> Self {
        Self {
            user_name: user_name.into(),
            database_name: None,
        }
    }

    pub fn with_database(mut self, database_name: impl Into<String>) -> Self {
        self.database_name = Some(database_name.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientDefaults {
    pub catalog_name: Option<String>,
    pub database_name: Option<String>,
}

impl ClientDefaults {
    pub fn with_catalog(mut self, catalog_name: impl Into<String>) -> Self {
        self.catalog_name = Some(catalog_name.into());
        self
    }

    pub fn with_database(mut self, database_name: impl Into<String>) -> Self {
        self.database_name = Some(database_name.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ClientCapabilities {
    pub supports_prepared_statements: bool,
    pub supports_portals: bool,
    pub supports_streaming_results: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConnectionContext {
    pub connection_id: Uuid,
    pub transport_kind: &'static str,
    pub peer: Option<String>,
}

impl ClientConnectionContext {
    pub fn new(connection_id: Uuid, transport_kind: &'static str) -> Self {
        Self {
            connection_id,
            transport_kind,
            peer: None,
        }
    }

    pub fn with_peer(mut self, peer: impl Into<String>) -> Self {
        self.peer = Some(peer.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientSessionContext {
    pub session_id: Uuid,
    pub identity: ClientIdentity,
}

impl ClientSessionContext {
    pub fn new(session_id: Uuid, identity: ClientIdentity) -> Self {
        Self {
            session_id,
            identity,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientContext {
    pub session: ClientSessionContext,
    pub connection: Option<ClientConnectionContext>,
    pub defaults: ClientDefaults,
    pub identity: ClientIdentity,
    pub capabilities: ClientCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenClientSession {
    pub auth: AuthContext,
    pub defaults: ClientDefaults,
    pub connection: Option<ClientConnectionContext>,
    pub capabilities: ClientCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenedClientSession {
    pub context: ClientContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub trace_id: Option<String>,
}

impl RequestContext {
    pub fn new(request_id: Uuid) -> Self {
        Self {
            request_id,
            trace_id: None,
        }
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientSqlRequest {
    pub client_context: ClientContext,
    pub request_context: RequestContext,
    pub sql: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementScope {
    SessionLocal,
    RuntimeBound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementRoute {
    pub scope: StatementScope,
}

pub trait StatementRouter {
    fn route(&self, sql: &str) -> Result<StatementRoute, FrontendError>;
}

#[derive(Clone, Debug, Default)]
pub struct FrontendService;

impl FrontendService {
    pub fn open_session<A: Authenticator>(
        &self,
        authenticator: &A,
        request: OpenClientSession,
    ) -> Result<OpenedClientSession, FrontendError> {
        let decision = authenticator.authenticate(&request.auth)?;
        let identity = ClientIdentity::new(decision.effective_user)
            .with_database_opt(decision.database_name.clone());
        let session = ClientSessionContext::new(Uuid::new_v4(), identity.clone());

        Ok(OpenedClientSession {
            context: ClientContext {
                session,
                connection: request.connection,
                defaults: request.defaults.with_database_opt(decision.database_name),
                identity,
                capabilities: request.capabilities,
            },
        })
    }

    pub fn build_request(
        &self,
        session: &OpenedClientSession,
        request_context: RequestContext,
        sql: impl Into<String>,
    ) -> Result<ClientSqlRequest, FrontendError> {
        let sql = sql.into();
        if sql.trim().is_empty() {
            return Err(FrontendError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        Ok(ClientSqlRequest {
            client_context: session.context.clone(),
            request_context,
            sql,
        })
    }

    pub fn build_sql_ingress_request(
        &self,
        request: &ClientSqlRequest,
        route: &StatementRoute,
    ) -> Result<SqlIngressRequest, FrontendError> {
        if request.sql.trim().is_empty() {
            return Err(FrontendError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        Ok(SqlIngressRequest {
            session: SqlSessionContext {
                session_id: request.client_context.session.session_id,
                user_name: request.client_context.identity.user_name.clone(),
                database_name: request.client_context.identity.database_name.clone(),
                catalog_name: request.client_context.defaults.catalog_name.clone(),
            },
            request: SqlRequestContext {
                request_id: request.request_context.request_id,
            },
            sql: request.sql.clone(),
            route: FrontendStatementRoute {
                scope: match route.scope {
                    StatementScope::SessionLocal => FrontendStatementRouteScope::SessionLocal,
                    StatementScope::RuntimeBound => FrontendStatementRouteScope::RuntimeBound,
                },
                statement_name: None,
            },
            client_capabilities: Some(SqlClientCapabilities {
                supports_prepared_statements: request
                    .client_context
                    .capabilities
                    .supports_prepared_statements,
                supports_portals: request.client_context.capabilities.supports_portals,
                supports_streaming_results: request
                    .client_context
                    .capabilities
                    .supports_streaming_results,
            }),
        })
    }
}

impl StatementRouter for FrontendService {
    fn route(&self, sql: &str) -> Result<StatementRoute, FrontendError> {
        let normalized = sql.trim();
        if normalized.is_empty() {
            return Err(FrontendError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        let upper = normalized.to_ascii_uppercase();
        let route = if upper.starts_with("SET ")
            || upper == "BEGIN"
            || upper == "COMMIT"
            || upper == "ROLLBACK"
            || upper.starts_with("SHOW ")
            || upper.starts_with("USE ")
        {
            StatementRoute {
                scope: StatementScope::SessionLocal,
            }
        } else {
            StatementRoute {
                scope: StatementScope::RuntimeBound,
            }
        };

        Ok(route)
    }
}

trait WithDatabaseOpt {
    fn with_database_opt(self, database_name: Option<String>) -> Self;
}

impl WithDatabaseOpt for ClientIdentity {
    fn with_database_opt(mut self, database_name: Option<String>) -> Self {
        self.database_name = database_name;
        self
    }
}

impl WithDatabaseOpt for ClientDefaults {
    fn with_database_opt(mut self, database_name: Option<String>) -> Self {
        self.database_name = database_name;
        self
    }
}

impl Default for ClientDefaults {
    fn default() -> Self {
        Self {
            catalog_name: None,
            database_name: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::auth::{AuthContext, AuthMethod, StaticAuthenticator};

    use super::{
        ClientCapabilities, ClientConnectionContext, ClientDefaults, FrontendService,
        OpenClientSession, RequestContext, StatementRouter, StatementScope,
    };

    #[test]
    fn router_keeps_set_inside_session_boundary() {
        let service = FrontendService;
        let route = service.route("set search_path = brew").unwrap();

        assert_eq!(route.scope, StatementScope::SessionLocal);
    }

    #[test]
    fn router_sends_select_into_runtime_path() {
        let service = FrontendService;
        let route = service.route("select 1").unwrap();

        assert_eq!(route.scope, StatementScope::RuntimeBound);
    }

    #[test]
    fn service_builds_request_for_opened_session() {
        let service = FrontendService;
        let opened = service
            .open_session(
                &StaticAuthenticator,
                OpenClientSession {
                    auth: AuthContext::new("brew", AuthMethod::Trust).with_database("brewdb"),
                    defaults: ClientDefaults::default().with_catalog("main"),
                    connection: Some(ClientConnectionContext::new(Uuid::nil(), "pgwire")),
                    capabilities: ClientCapabilities::default(),
                },
            )
            .unwrap();

        let request = service
            .build_request(&opened, RequestContext::new(Uuid::nil()), "select 1")
            .unwrap();

        assert_eq!(request.client_context.identity.user_name, "brew");
        assert_eq!(
            request.client_context.identity.database_name.as_deref(),
            Some("brewdb")
        );
        assert_eq!(request.sql, "select 1");
    }
}

#[cfg(test)]
mod sql_handoff_tests {
    use uuid::Uuid;

    use brewdb_sql::{FrontendStatementRouteScope, SqlClientCapabilities, SqlIngressRequest};

    use crate::session::{
        ClientCapabilities, ClientContext, ClientDefaults, ClientIdentity, ClientSessionContext,
        ClientSqlRequest, FrontendService, RequestContext, StatementRoute, StatementScope,
    };

    fn make_client_request(sql: &str) -> ClientSqlRequest {
        ClientSqlRequest {
            client_context: ClientContext {
                session: ClientSessionContext::new(
                    Uuid::nil(),
                    ClientIdentity::new("brew").with_database("brewdb"),
                ),
                connection: None,
                defaults: ClientDefaults::default().with_catalog("main"),
                identity: ClientIdentity::new("brew").with_database("brewdb"),
                capabilities: ClientCapabilities {
                    supports_prepared_statements: true,
                    supports_portals: false,
                    supports_streaming_results: true,
                },
            },
            request_context: RequestContext::new(Uuid::nil()),
            sql: sql.to_string(),
        }
    }

    #[test]
    fn client_sql_request_maps_to_sql_ingress_request() {
        let request = make_client_request("select 1");
        let route = StatementRoute {
            scope: StatementScope::RuntimeBound,
        };

        let sql_request = FrontendService
            .build_sql_ingress_request(&request, &route)
            .unwrap();

        assert_eq!(sql_request.session.session_id, Uuid::nil());
        assert_eq!(sql_request.session.user_name, "brew");
        assert_eq!(sql_request.session.database_name.as_deref(), Some("brewdb"));
        assert_eq!(sql_request.session.catalog_name.as_deref(), Some("main"));
        assert_eq!(sql_request.request.request_id, Uuid::nil());
        assert_eq!(sql_request.sql, "select 1");
        assert_eq!(
            sql_request.route.scope,
            FrontendStatementRouteScope::RuntimeBound
        );
        assert_eq!(sql_request.route.statement_name, None);
        assert_eq!(
            sql_request.client_capabilities,
            Some(SqlClientCapabilities {
                supports_prepared_statements: true,
                supports_portals: false,
                supports_streaming_results: true,
            })
        );
    }

    #[test]
    fn sql_ingress_request_does_not_include_connection_transport_data() {
        let request = make_client_request("set search_path = brew");
        let route = StatementRoute {
            scope: StatementScope::SessionLocal,
        };

        let sql_request: SqlIngressRequest = FrontendService
            .build_sql_ingress_request(&request, &route)
            .unwrap();

        assert_eq!(
            sql_request.route.scope,
            FrontendStatementRouteScope::SessionLocal
        );
        assert_eq!(sql_request.route.statement_name, None);
        assert_eq!(std::mem::size_of_val(&sql_request.session.session_id), 16);
    }
}
