//! Protocol-neutral session and request handling.

use crate::common::config::ConfigSet;
use crate::common::context::QueryContext;
use tracing::info;
use uuid::Uuid;

use crate::frontend::auth::{AuthContext, Authenticator};
use crate::frontend::errors::FrontendError;

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

#[derive(Clone, Debug, PartialEq, Eq, Default)]
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
    pub settings: ConfigSet,
}

impl ClientContext {
    pub fn query_context(&self, query_id: Uuid) -> QueryContext {
        QueryContext::new(
            query_id,
            self.session.session_id,
            self.identity.user_name.clone(),
            self.identity.database_name.clone(),
            self.defaults.catalog_name.clone(),
            self.settings.clone(),
        )
    }
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
pub struct SqlRequest {
    pub query_context: QueryContext,
    pub sql: String,
}

#[derive(Clone, Debug, Default)]
pub struct FrontendService {
    // TODO: Add a frontend-owned SessionManager that stores ClientContext by
    // session_id. QueryContext should continue to be a flattened per-query
    // snapshot derived from the client session, not a holder of frontend state.
    system_settings: ConfigSet,
}

impl FrontendService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_system_settings(system_settings: ConfigSet) -> Self {
        Self { system_settings }
    }

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
                settings: self.system_settings.clone(),
            },
        })
    }

    pub fn build_request(
        &self,
        session: &OpenedClientSession,
        query_id: Uuid,
        sql: impl Into<String>,
    ) -> Result<SqlRequest, FrontendError> {
        let sql = sql.into();
        if sql.trim().is_empty() {
            return Err(FrontendError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            });
        }

        let query_context = session.context.query_context(query_id);
        let _guard = query_context.span().entered();
        info!(target: "brewdb.frontend", sql_len = sql.len(), "frontend received query");

        Ok(SqlRequest { query_context, sql })
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::common::config::ConfigSet;
    use crate::frontend::MANAGED_PAIMON_CATALOG_NAME;
    use crate::frontend::auth::{AuthContext, AuthMethod, StaticAuthenticator};

    use super::{
        ClientCapabilities, ClientConnectionContext, ClientDefaults, FrontendService,
        OpenClientSession,
    };

    #[test]
    fn service_builds_request_for_opened_session() {
        let service = FrontendService::new();
        let opened = service
            .open_session(
                &StaticAuthenticator,
                OpenClientSession {
                    auth: AuthContext::new("brew", AuthMethod::Trust).with_database("brewdb"),
                    defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
                    connection: Some(ClientConnectionContext::new(Uuid::nil(), "pgwire")),
                    capabilities: ClientCapabilities::default(),
                },
            )
            .unwrap();

        let request = service
            .build_request(&opened, Uuid::nil(), "select 1")
            .unwrap();

        assert_eq!(request.query_context.user_name, "brew");
        assert_eq!(
            request.query_context.database_name.as_deref(),
            Some("brewdb")
        );
        assert_eq!(request.sql, "select 1");
    }

    #[test]
    fn service_builds_session_settings_from_system_settings() {
        let service = FrontendService::with_system_settings(
            ConfigSet::new().with_entry("datafusion.execution.batch_size", 128_u64),
        );
        let opened = service
            .open_session(
                &StaticAuthenticator,
                OpenClientSession {
                    auth: AuthContext::new("brew", AuthMethod::Trust).with_database("brewdb"),
                    defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
                    connection: None,
                    capabilities: ClientCapabilities::default(),
                },
            )
            .unwrap();
        let request = service
            .build_request(&opened, Uuid::nil(), "select 1")
            .unwrap();

        assert_eq!(
            request
                .query_context
                .settings
                .get_u64("datafusion.execution.batch_size")
                .unwrap(),
            Some(128)
        );
    }
}

#[cfg(test)]
mod sql_handoff_tests {
    use uuid::Uuid;

    use crate::common::config::ConfigSet;
    use crate::frontend::MANAGED_PAIMON_CATALOG_NAME;
    use crate::frontend::session::{
        ClientCapabilities, ClientContext, ClientDefaults, ClientIdentity, ClientSessionContext,
        SqlRequest,
    };

    fn make_sql_request(sql: &str) -> SqlRequest {
        let client_context = ClientContext {
            session: ClientSessionContext::new(
                Uuid::nil(),
                ClientIdentity::new("brew").with_database("brewdb"),
            ),
            connection: None,
            defaults: ClientDefaults::default().with_catalog(MANAGED_PAIMON_CATALOG_NAME),
            identity: ClientIdentity::new("brew").with_database("brewdb"),
            capabilities: ClientCapabilities {
                supports_prepared_statements: true,
                supports_portals: false,
                supports_streaming_results: true,
            },
            settings: ConfigSet::new(),
        };
        SqlRequest {
            query_context: client_context.query_context(Uuid::nil()),
            sql: sql.to_string(),
        }
    }

    #[test]
    fn sql_request_carries_query_context() {
        let request = make_sql_request("select 1");

        assert_eq!(request.query_context.session_id, Uuid::nil());
        assert_eq!(request.query_context.user_name, "brew");
        assert_eq!(
            request.query_context.database_name.as_deref(),
            Some("brewdb")
        );
        assert_eq!(
            request.query_context.catalog_name.as_deref(),
            Some(MANAGED_PAIMON_CATALOG_NAME)
        );
        assert_eq!(request.query_context.query_id, Uuid::nil());
        assert_eq!(request.sql, "select 1");
    }

    #[test]
    fn query_context_does_not_keep_frontend_client_context() {
        let request = make_sql_request("set search_path = brew");

        assert_eq!(std::mem::size_of_val(&request.query_context.session_id), 16);
    }
}
