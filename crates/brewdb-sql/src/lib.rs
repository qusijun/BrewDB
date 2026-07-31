//! BrewDB SQL entry and statement-classification shell.

pub mod errors;
pub mod ingress;
pub mod statement;

pub use errors::SqlError;
pub use ingress::{
    FrontendStatementRoute, FrontendStatementRouteScope, SqlClientCapabilities, SqlDriver,
    SqlIngressRequest, SqlRequestContext, SqlSessionContext,
};
pub use statement::{
    RuntimeStatement, SessionStatement, SqlStatementEnvelope, StatementCategory, StatementPayload,
};

#[cfg(test)]
mod ingress_tests {
    use uuid::Uuid;

    use crate::errors::SqlError;
    use crate::ingress::{
        FrontendStatementRoute, FrontendStatementRouteScope, SqlDriver, SqlIngressRequest,
        SqlRequestContext, SqlSessionContext,
    };
    use crate::statement::{StatementCategory, StatementPayload};

    fn make_request(sql: &str, scope: FrontendStatementRouteScope) -> SqlIngressRequest {
        SqlIngressRequest {
            session: SqlSessionContext {
                session_id: Uuid::nil(),
                user_name: "brew".to_string(),
                database_name: Some("brewdb".to_string()),
                catalog_name: Some("main".to_string()),
            },
            request: SqlRequestContext {
                request_id: Uuid::nil(),
            },
            sql: sql.to_string(),
            route: FrontendStatementRoute {
                scope,
                statement_name: None,
            },
            client_capabilities: None,
        }
    }

    #[test]
    fn analyze_returns_session_statement_for_set_scope() {
        let driver = SqlDriver::default();
        let envelope = driver
            .analyze(make_request(
                "set search_path = brew",
                FrontendStatementRouteScope::SessionLocal,
            ))
            .unwrap();

        assert_eq!(envelope.category, StatementCategory::Session);
        assert!(matches!(envelope.payload, StatementPayload::Session(_)));
        assert_eq!(envelope.statement_name, None);
    }

    #[test]
    fn analyze_returns_runtime_statement_for_select_scope() {
        let driver = SqlDriver::default();
        let envelope = driver
            .analyze(make_request(
                "select 1",
                FrontendStatementRouteScope::RuntimeBound,
            ))
            .unwrap();

        assert_eq!(envelope.category, StatementCategory::Runtime);
        assert!(matches!(envelope.payload, StatementPayload::Runtime(_)));
        assert_eq!(envelope.statement_name, None);
    }

    #[test]
    fn analyze_rejects_empty_sql() {
        let driver = SqlDriver::default();
        let error = driver
            .analyze(make_request(
                "   ",
                FrontendStatementRouteScope::RuntimeBound,
            ))
            .unwrap_err();

        assert_eq!(
            error,
            SqlError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn analyze_preserves_frontend_statement_name_when_present() {
        let driver = SqlDriver::default();
        let mut request = make_request("select 1", FrontendStatementRouteScope::RuntimeBound);
        request.route.statement_name = Some("SELECT".to_string());

        let envelope = driver.analyze(request).unwrap();

        assert_eq!(envelope.statement_name, Some("SELECT".to_string()));
    }
}
