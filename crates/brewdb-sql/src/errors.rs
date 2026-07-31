//! SQL ingress errors.

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
