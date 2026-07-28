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

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::LogicalTableName;
    use brewdb_core::common::RequestContext;

    use super::{FrontendSqlRequest, QueryIntent, SqlIntent, StatementClass};

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
}
