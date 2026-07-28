//! SQL frontend entry and intent families.

use brewdb_core::catalog::LogicalTableName;
use brewdb_core::common::RequestContext;

use crate::errors::SqlError;

/// High-level statement classes surfaced by the SQL frontend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatementClass {
    Query,
    Insert,
    Mutation,
    Maintenance,
    Ddl,
}

/// Query intent family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryIntent {
    pub statement_label: String,
    pub reads: Vec<LogicalTableName>,
}

/// Insert intent family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertIntent {
    pub target: LogicalTableName,
    pub source_query_label: String,
}

/// Mutation intent family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationIntent {
    pub target: LogicalTableName,
    pub operation: String,
}

/// Maintenance intent family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintenanceIntent {
    pub target: LogicalTableName,
    pub operation: String,
}

/// DDL intent family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DdlIntent {
    pub target: Option<LogicalTableName>,
    pub operation: String,
}

/// Unified SQL frontend output boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlIntent {
    Query(QueryIntent),
    Insert(InsertIntent),
    Mutation(MutationIntent),
    Maintenance(MaintenanceIntent),
    Ddl(DdlIntent),
}

impl SqlIntent {
    pub fn statement_class(&self) -> StatementClass {
        match self {
            Self::Query(_) => StatementClass::Query,
            Self::Insert(_) => StatementClass::Insert,
            Self::Mutation(_) => StatementClass::Mutation,
            Self::Maintenance(_) => StatementClass::Maintenance,
            Self::Ddl(_) => StatementClass::Ddl,
        }
    }
}

/// SQL frontend request entering parse/bind/analyze/rewrite pipelines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendSqlRequest {
    pub sql: String,
    pub request_context: RequestContext,
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
}

/// SQL frontend result after capability gate and intent emission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendSqlResult {
    pub statement_class: StatementClass,
    pub intent: SqlIntent,
}

/// Capability gate shell before intent emission.
pub trait CapabilityGate {
    fn check_capability(&self, intent: &SqlIntent) -> Result<(), SqlError>;
}

/// Statement-to-intent entry boundary.
pub trait IntentPlanner {
    fn build_intent(&self, request: FrontendSqlRequest) -> Result<FrontendSqlResult, SqlError>;
}

/// Phase 1 query-first planner shell.
///
/// This planner intentionally recognizes only query-shaped statements so the
/// first closed loop can stay focused on the read path.
#[derive(Clone, Debug, Default)]
pub struct QueryOnlyIntentPlanner;

impl IntentPlanner for QueryOnlyIntentPlanner {
    fn build_intent(&self, request: FrontendSqlRequest) -> Result<FrontendSqlResult, SqlError> {
        let sql = request.sql.trim();
        if sql.is_empty() {
            return Err(SqlError::MissingField {
                entity: "frontend_sql_request",
                field: "sql",
            });
        }

        let normalized = sql.to_ascii_lowercase();
        if !normalized.starts_with("select") && !normalized.starts_with("with") {
            return Err(SqlError::Unsupported {
                operation: "non_query_statement",
                reason: "phase 1 query skeleton only accepts query-shaped SQL".to_owned(),
            });
        }

        let intent = SqlIntent::Query(QueryIntent {
            statement_label: statement_label(sql),
            reads: Vec::new(),
        });

        Ok(FrontendSqlResult {
            statement_class: intent.statement_class(),
            intent,
        })
    }
}

fn statement_label(sql: &str) -> String {
    sql.split_whitespace()
        .next()
        .map(|token| token.to_ascii_lowercase())
        .unwrap_or_else(|| "query".to_owned())
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::LogicalTableName;
    use brewdb_core::common::RequestContext;

    use super::{
        FrontendSqlRequest, IntentPlanner, QueryIntent, QueryOnlyIntentPlanner, SqlIntent,
        StatementClass,
    };

    #[test]
    fn sql_intent_reports_statement_class() {
        let intent = SqlIntent::Query(QueryIntent {
            statement_label: "select".to_owned(),
            reads: vec![LogicalTableName {
                catalog_name: "brew".to_owned(),
                database_name: "analytics".to_owned(),
                table_name: "orders".to_owned(),
            }],
        });

        assert_eq!(intent.statement_class(), StatementClass::Query);
    }

    #[test]
    fn frontend_request_keeps_default_catalog_and_database() {
        let request = FrontendSqlRequest {
            sql: "select * from orders".to_owned(),
            request_context: RequestContext::new(),
            default_catalog: Some("brew".to_owned()),
            default_database: Some("analytics".to_owned()),
        };

        assert_eq!(request.default_catalog.as_deref(), Some("brew"));
        assert_eq!(request.default_database.as_deref(), Some("analytics"));
    }

    #[test]
    fn query_only_planner_accepts_select() {
        let planner = QueryOnlyIntentPlanner;
        let result = planner
            .build_intent(FrontendSqlRequest {
                sql: "SELECT * FROM orders".to_owned(),
                request_context: RequestContext::new(),
                default_catalog: None,
                default_database: None,
            })
            .unwrap();

        assert_eq!(result.statement_class, StatementClass::Query);
        assert!(matches!(result.intent, SqlIntent::Query(_)));
    }

    #[test]
    fn query_only_planner_rejects_insert() {
        let planner = QueryOnlyIntentPlanner;
        let error = planner
            .build_intent(FrontendSqlRequest {
                sql: "INSERT INTO orders VALUES (1)".to_owned(),
                request_context: RequestContext::new(),
                default_catalog: None,
                default_database: None,
            })
            .unwrap_err();

        assert!(matches!(error, SqlError::Unsupported { .. }));
    }
}
