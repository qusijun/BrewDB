//! Frontend protocol error surface.

use std::error::Error;
use std::fmt;

use brewdb_core::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontendError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    Unsupported {
        operation: &'static str,
        reason: String,
    },
}

impl fmt::Display for FrontendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::Unsupported { operation, reason } => {
                write!(f, "unsupported frontend operation `{operation}`: {reason}")
            }
        }
    }
}

impl Error for FrontendError {}

impl FrontendError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingField { .. } => ErrorCode::FrontendMissingField,
            Self::Unsupported { .. } => ErrorCode::FrontendUnsupported,
        }
    }
}

impl DiagnosticError for FrontendError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.frontend"
    }
}
