//! Shared foundational error helpers.

use std::error::Error;
use std::fmt;

use crate::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommonError {
    InvalidConfiguration { field: String, reason: String },
    LoggingInitializationFailed { reason: String },
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration { field, reason } => {
                write!(f, "invalid configuration for `{field}`: {reason}")
            }
            Self::LoggingInitializationFailed { reason } => {
                write!(f, "logging initialization failed: {reason}")
            }
        }
    }
}

impl Error for CommonError {}

impl DiagnosticError for CommonError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidConfiguration { .. } => ErrorCode::InvalidConfiguration,
            Self::LoggingInitializationFailed { .. } => ErrorCode::LoggingInitializationFailed,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.common"
    }
}
