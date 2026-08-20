//! Shared foundational error helpers.

use std::error::Error;
use std::fmt;

use crate::common::diagnostics::{DiagnosticError, ErrorCode};

const COMMON_INVALID_CONFIGURATION: ErrorCode = ErrorCode::INVALID_CONFIGURATION;
const COMMON_LOGGING_INITIALIZATION_FAILED: ErrorCode = ErrorCode::LOGGING_INITIALIZATION_FAILED;
const COMMON_SCHEMA_CONVERSION_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_COMMON_SCHEMA_CONVERSION_FAILED");
const COMMON_INVALID_TABLE_REFERENCE: ErrorCode =
    ErrorCode::new("BREWDB_COMMON_INVALID_TABLE_REFERENCE");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommonError {
    InvalidConfiguration { field: String, reason: String },
    LoggingInitializationFailed { reason: String },
    SchemaConversionFailed { reason: String },
    InvalidTableReference { reference: String },
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
            Self::SchemaConversionFailed { reason } => {
                write!(f, "schema conversion failed: {reason}")
            }
            Self::InvalidTableReference { reference } => {
                write!(f, "table reference must be fully qualified: {reference}")
            }
        }
    }
}

impl Error for CommonError {}

impl DiagnosticError for CommonError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidConfiguration { .. } => COMMON_INVALID_CONFIGURATION,
            Self::LoggingInitializationFailed { .. } => COMMON_LOGGING_INITIALIZATION_FAILED,
            Self::SchemaConversionFailed { .. } => COMMON_SCHEMA_CONVERSION_FAILED,
            Self::InvalidTableReference { .. } => COMMON_INVALID_TABLE_REFERENCE,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.common"
    }
}
