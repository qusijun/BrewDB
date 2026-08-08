//! SQL parse and bind errors.

use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};
use datafusion_sql::sqlparser::parser::ParserError;

const SQL_INVALID_REQUEST: ErrorCode = ErrorCode::new("BREWDB_SQL_INVALID_REQUEST");
const SQL_PARSE_FAILED: ErrorCode = ErrorCode::new("BREWDB_SQL_PARSE_FAILED");
const SQL_UNSUPPORTED_STATEMENT: ErrorCode = ErrorCode::new("BREWDB_SQL_UNSUPPORTED_STATEMENT");
const SQL_MISSING_DEFAULT_CATALOG: ErrorCode = ErrorCode::new("BREWDB_SQL_MISSING_DEFAULT_CATALOG");
const SQL_MISSING_DEFAULT_DATABASE: ErrorCode =
    ErrorCode::new("BREWDB_SQL_MISSING_DEFAULT_DATABASE");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlError {
    InvalidRequest { reason: String },
    Parse { reason: String },
    UnsupportedStatement { reason: String },
    MissingDefaultCatalog,
    MissingDefaultDatabase,
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { reason } => write!(f, "invalid sql request: {reason}"),
            Self::Parse { reason } => write!(f, "sql parse failed: {reason}"),
            Self::UnsupportedStatement { reason } => write!(f, "unsupported statement: {reason}"),
            Self::MissingDefaultCatalog => write!(f, "missing default catalog in session context"),
            Self::MissingDefaultDatabase => {
                write!(f, "missing default database in session context")
            }
        }
    }
}

impl Error for SqlError {}

impl DiagnosticError for SqlError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidRequest { .. } => SQL_INVALID_REQUEST,
            Self::Parse { .. } => SQL_PARSE_FAILED,
            Self::UnsupportedStatement { .. } => SQL_UNSUPPORTED_STATEMENT,
            Self::MissingDefaultCatalog => SQL_MISSING_DEFAULT_CATALOG,
            Self::MissingDefaultDatabase => SQL_MISSING_DEFAULT_DATABASE,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.sql"
    }
}

impl From<ParserError> for SqlError {
    fn from(value: ParserError) -> Self {
        Self::Parse {
            reason: value.to_string(),
        }
    }
}
