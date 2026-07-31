//! SQL ingress errors.

use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlError {
    InvalidRequest { reason: String },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { reason } => write!(f, "invalid sql ingress request: {reason}"),
        }
    }
}

impl Error for SqlError {}

impl DiagnosticError for SqlError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidRequest { .. } => ErrorCode::InvalidConfiguration,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.sql"
    }
}
