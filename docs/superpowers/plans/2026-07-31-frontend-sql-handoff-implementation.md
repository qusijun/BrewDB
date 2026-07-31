# Frontend to SQL Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a protocol-neutral `brewdb-frontend -> brewdb-sql` ingress boundary that classifies statements through SQL-owned contracts without leaking pgwire-specific details.

**Architecture:** `brewdb-sql` becomes the owner of ingress validation and statement-envelope truth through new `ingress`, `statement`, and `errors` modules. `brewdb-frontend` keeps owning client/session/request truth, and adds a narrow adapter that maps `ClientSqlRequest` plus frontend routing into `SqlIngressRequest`.

**Tech Stack:** Rust 2024 workspace crates, `uuid`, existing BrewDB common diagnostics helpers, crate-local unit tests via `cargo test --offline`

---

## File Structure

### `crates/brewdb-sql`

- Modify: `crates/brewdb-sql/Cargo.toml`
  - add `uuid.workspace = true`
- Modify: `crates/brewdb-sql/src/lib.rs`
  - export new SQL ingress modules and public types
- Create: `crates/brewdb-sql/src/errors.rs`
  - SQL ingress error family
- Create: `crates/brewdb-sql/src/statement.rs`
  - statement categories and outward envelope
- Create: `crates/brewdb-sql/src/ingress.rs`
  - frontend-facing SQL ingress request types and `SqlDriver`

### `crates/brewdb-frontend`

- Modify: `crates/brewdb-frontend/src/lib.rs`
  - keep frontend public exports aligned after adding the SQL handoff adapter
- Modify: `crates/brewdb-frontend/src/session/mod.rs`
  - add adapter method from `ClientSqlRequest` to `SqlIngressRequest`
  - add focused tests for protocol-neutral mapping

## Task 1: Add Failing SQL Ingress Tests

**Files:**
- Modify: `crates/brewdb-sql/src/lib.rs`
- Create: `crates/brewdb-sql/src/ingress.rs`
- Create: `crates/brewdb-sql/src/statement.rs`
- Create: `crates/brewdb-sql/src/errors.rs`

- [ ] **Step 1: Write the failing SQL ingress tests**

```rust
// crates/brewdb-sql/src/ingress.rs
#[cfg(test)]
mod tests {
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
            .analyze(make_request("set search_path = brew", FrontendStatementRouteScope::SessionLocal))
            .unwrap();

        assert_eq!(envelope.category, StatementCategory::Session);
        assert!(matches!(envelope.payload, StatementPayload::Session(_)));
        assert_eq!(envelope.statement_name, "SET");
    }

    #[test]
    fn analyze_returns_runtime_statement_for_select_scope() {
        let driver = SqlDriver::default();
        let envelope = driver
            .analyze(make_request("select 1", FrontendStatementRouteScope::RuntimeBound))
            .unwrap();

        assert_eq!(envelope.category, StatementCategory::Runtime);
        assert!(matches!(envelope.payload, StatementPayload::Runtime(_)));
        assert_eq!(envelope.statement_name, "SELECT");
    }

    #[test]
    fn analyze_rejects_empty_sql() {
        let driver = SqlDriver::default();
        let error = driver
            .analyze(make_request("   ", FrontendStatementRouteScope::RuntimeBound))
            .unwrap_err();

        assert_eq!(
            error,
            SqlError::InvalidRequest {
                reason: "SQL text must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn analyze_rejects_route_conflict() {
        let driver = SqlDriver::default();
        let error = driver
            .analyze(make_request("select 1", FrontendStatementRouteScope::SessionLocal))
            .unwrap_err();

        assert_eq!(
            error,
            SqlError::RouteConflict {
                sql_statement_name: "SELECT".to_string(),
                frontend_scope: FrontendStatementRouteScope::SessionLocal,
                sql_scope: FrontendStatementRouteScope::RuntimeBound,
            }
        );
    }

    #[test]
    fn analyze_rejects_unsupported_statement() {
        let driver = SqlDriver::default();
        let error = driver
            .analyze(make_request("merge into brewdb", FrontendStatementRouteScope::RuntimeBound))
            .unwrap_err();

        assert_eq!(
            error,
            SqlError::UnsupportedStatement {
                statement_name: "MERGE".to_string(),
            }
        );
    }
}
```

- [ ] **Step 2: Run the SQL tests to verify they fail**

Run: `cargo test -p brewdb-sql --offline ingress::tests`

Expected: FAIL with missing modules and missing `SqlDriver` / `SqlIngressRequest` / `SqlError` types

- [ ] **Step 3: Write the minimal SQL contract implementation**

```rust
// crates/brewdb-sql/src/lib.rs
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
```

```rust
// crates/brewdb-sql/src/errors.rs
use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};

use crate::ingress::FrontendStatementRouteScope;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlError {
    InvalidRequest { reason: String },
    RouteConflict {
        sql_statement_name: String,
        frontend_scope: FrontendStatementRouteScope,
        sql_scope: FrontendStatementRouteScope,
    },
    UnsupportedStatement { statement_name: String },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { reason } => write!(f, "invalid sql ingress request: {reason}"),
            Self::RouteConflict {
                sql_statement_name,
                frontend_scope,
                sql_scope,
            } => write!(
                f,
                "frontend route conflict for `{sql_statement_name}`: frontend={frontend_scope:?}, sql={sql_scope:?}"
            ),
            Self::UnsupportedStatement { statement_name } => {
                write!(f, "unsupported statement: {statement_name}")
            }
        }
    }
}

impl Error for SqlError {}

impl DiagnosticError for SqlError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidRequest { .. } => ErrorCode::InvalidConfiguration,
            Self::RouteConflict { .. } => ErrorCode::InvalidConfiguration,
            Self::UnsupportedStatement { .. } => ErrorCode::NotImplemented,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.sql"
    }
}
```

```rust
// crates/brewdb-sql/src/statement.rs
use crate::ingress::FrontendStatementRouteScope;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementCategory {
    Session,
    Runtime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatement {
    pub statement_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatement {
    pub statement_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatementPayload {
    Session(SessionStatement),
    Runtime(RuntimeStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlStatementEnvelope {
    pub statement_text: String,
    pub statement_name: String,
    pub category: StatementCategory,
    pub route_scope: FrontendStatementRouteScope,
    pub payload: StatementPayload,
}
```

```rust
// crates/brewdb-sql/src/ingress.rs
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

        let statement_name = classify_statement_name(&statement_text);
        if statement_name == "UNKNOWN" || statement_name == "MERGE" {
            return Err(SqlError::UnsupportedStatement {
                statement_name: statement_name.to_string(),
            });
        }

        let sql_scope = infer_scope(statement_name);

        if request.route.scope != sql_scope {
            return Err(SqlError::RouteConflict {
                sql_statement_name: statement_name.to_string(),
                frontend_scope: request.route.scope,
                sql_scope,
            });
        }

        let (category, payload) = match sql_scope {
            FrontendStatementRouteScope::SessionLocal => (
                StatementCategory::Session,
                StatementPayload::Session(SessionStatement {
                    statement_name: statement_name.to_string(),
                }),
            ),
            FrontendStatementRouteScope::RuntimeBound => (
                StatementCategory::Runtime,
                StatementPayload::Runtime(RuntimeStatement {
                    statement_name: statement_name.to_string(),
                }),
            ),
        };

        Ok(SqlStatementEnvelope {
            statement_text,
            statement_name: statement_name.to_string(),
            category,
            route_scope: sql_scope,
            payload,
        })
    }
}

fn classify_statement_name(sql: &str) -> &'static str {
    match sql.split_whitespace().next().unwrap_or("UNKNOWN").to_ascii_uppercase().as_str() {
        "SELECT" => "SELECT",
        "SET" => "SET",
        "SHOW" => "SHOW",
        "USE" => "USE",
        "BEGIN" => "BEGIN",
        "COMMIT" => "COMMIT",
        "ROLLBACK" => "ROLLBACK",
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        _ => "UNKNOWN",
    }
}

fn infer_scope(statement_name: &str) -> FrontendStatementRouteScope {
    match statement_name {
        "SET" | "SHOW" | "USE" | "BEGIN" | "COMMIT" | "ROLLBACK" => {
            FrontendStatementRouteScope::SessionLocal
        }
        _ => FrontendStatementRouteScope::RuntimeBound,
    }
}
```

- [ ] **Step 4: Run the SQL tests to verify they pass**

Run: `cargo test -p brewdb-sql --offline ingress::tests`

Expected: PASS with 5 passed, 0 failed

- [ ] **Step 5: Commit the SQL ingress contract**

```bash
git add crates/brewdb-sql/Cargo.toml crates/brewdb-sql/src/lib.rs crates/brewdb-sql/src/errors.rs crates/brewdb-sql/src/ingress.rs crates/brewdb-sql/src/statement.rs
git commit -m "feat: add sql frontend ingress contract"
```

## Task 2: Add Failing Frontend Mapping Tests

**Files:**
- Modify: `crates/brewdb-frontend/src/session/mod.rs`

- [ ] **Step 1: Write the failing frontend mapping tests**

```rust
#[cfg(test)]
mod sql_handoff_tests {
    use uuid::Uuid;

    use brewdb_sql::{FrontendStatementRouteScope, SqlIngressRequest};

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
            statement_name: "SELECT",
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
        assert_eq!(sql_request.route.scope, FrontendStatementRouteScope::RuntimeBound);
        assert_eq!(
            sql_request.client_capabilities,
            Some(brewdb_sql::SqlClientCapabilities {
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
            statement_name: "SET",
        };

        let sql_request: SqlIngressRequest = FrontendService
            .build_sql_ingress_request(&request, &route)
            .unwrap();

        assert_eq!(sql_request.route.scope, FrontendStatementRouteScope::SessionLocal);
        assert_eq!(sql_request.route.statement_name.as_deref(), Some("SET"));
        assert_eq!(std::mem::size_of_val(&sql_request.session.session_id), 16);
    }
}
```

- [ ] **Step 2: Run the frontend tests to verify they fail**

Run: `cargo test -p brewdb-frontend --offline sql_handoff_tests`

Expected: FAIL with missing `build_sql_ingress_request` method and missing `brewdb_sql` imports or types

- [ ] **Step 3: Write the minimal frontend handoff adapter**

```rust
// crates/brewdb-frontend/src/session/mod.rs
use brewdb_sql::{
    FrontendStatementRoute, FrontendStatementRouteScope, SqlClientCapabilities, SqlIngressRequest,
    SqlRequestContext, SqlSessionContext,
};

impl FrontendService {
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
                statement_name: Some(route.statement_name.to_string()),
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
```

- [ ] **Step 4: Run the frontend tests to verify they pass**

Run: `cargo test -p brewdb-frontend --offline sql_handoff_tests`

Expected: PASS with 2 passed, 0 failed

- [ ] **Step 5: Commit the frontend handoff adapter**

```bash
git add crates/brewdb-frontend/src/session/mod.rs crates/brewdb-frontend/src/lib.rs
git commit -m "feat: map frontend requests into sql ingress"
```

## Task 3: Verify the Full Boundary

**Files:**
- Modify: `crates/brewdb-sql/Cargo.toml`
- Modify: `crates/brewdb-frontend/src/session/mod.rs`

- [ ] **Step 1: Add the missing dependency and exports**

```toml
# crates/brewdb-sql/Cargo.toml
[dependencies]
brewdb-common = { path = "../brewdb-common" }
brewdb-catalog = { path = "../brewdb-catalog" }
brewdb-runtime = { path = "../brewdb-runtime" }
uuid.workspace = true
```

```rust
// crates/brewdb-frontend/src/lib.rs
pub use session::{
    ClientCapabilities, ClientConnectionContext, ClientContext, ClientDefaults, ClientIdentity,
    ClientSessionContext, ClientSqlRequest, FrontendService, OpenClientSession,
    OpenedClientSession, RequestContext, StatementRoute, StatementRouter, StatementScope,
};
```

- [ ] **Step 2: Run crate-level tests for both sides**

Run: `cargo test -p brewdb-sql --offline`
Expected: PASS with 5 ingress tests green

Run: `cargo test -p brewdb-frontend --offline`
Expected: PASS with existing 7 tests plus new handoff tests green

- [ ] **Step 3: Run formatting**

Run: `cargo fmt --package brewdb-sql --package brewdb-frontend`

Expected: exit code 0 with no formatting errors

- [ ] **Step 4: Re-run the final verification**

Run: `cargo test -p brewdb-sql -p brewdb-frontend --offline`

Expected: PASS with 0 failures across both crates

- [ ] **Step 5: Commit the verified boundary**

```bash
git add crates/brewdb-sql/Cargo.toml crates/brewdb-sql/src/lib.rs crates/brewdb-sql/src/errors.rs crates/brewdb-sql/src/ingress.rs crates/brewdb-sql/src/statement.rs crates/brewdb-frontend/src/lib.rs crates/brewdb-frontend/src/session/mod.rs
git commit -m "feat: wire frontend sql handoff boundary"
```

## Spec Coverage Check

- formal SQL ingress contract: covered by Task 1
- SQL-owned statement envelope: covered by Task 1
- frontend mapping from `ClientSqlRequest`: covered by Task 2
- protocol-neutral field boundary: covered by Task 2 and Task 3 verification
- unsupported-statement error path: covered by Task 1
- crate-level validation and formatting: covered by Task 3
