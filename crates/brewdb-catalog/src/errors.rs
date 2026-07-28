//! Catalog-layer error surface.

use std::error::Error;
use std::fmt;

use brewdb_core::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    NotFound {
        entity: &'static str,
        key: String,
    },
    Unsupported {
        operation: &'static str,
        reason: String,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::NotFound { entity, key } => write!(f, "{entity} not found: {key}"),
            Self::Unsupported { operation, reason } => {
                write!(f, "unsupported catalog operation `{operation}`: {reason}")
            }
        }
    }
}

impl Error for CatalogError {}

impl CatalogError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingField { .. } => ErrorCode::CatalogMissingField,
            Self::NotFound { .. } => ErrorCode::CatalogNotFound,
            Self::Unsupported { .. } => ErrorCode::CatalogUnsupported,
        }
    }
}

impl DiagnosticError for CatalogError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.catalog"
    }
}
