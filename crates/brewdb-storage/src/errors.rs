//! Storage adapter error surface.

use std::error::Error;
use std::fmt;

use brewdb_core::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    Unsupported {
        operation: &'static str,
        reason: String,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::Unsupported { operation, reason } => {
                write!(f, "unsupported storage operation `{operation}`: {reason}")
            }
        }
    }
}

impl Error for StorageError {}

impl StorageError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingField { .. } => ErrorCode::StorageMissingField,
            Self::Unsupported { .. } => ErrorCode::StorageUnsupported,
        }
    }
}

impl DiagnosticError for StorageError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.storage"
    }
}
