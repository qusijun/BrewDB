//! SQL frontend error surface.

use std::error::Error;
use std::fmt;

use brewdb_core::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    Unsupported {
        operation: &'static str,
        reason: String,
    },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::Unsupported { operation, reason } => {
                write!(f, "unsupported sql operation `{operation}`: {reason}")
            }
        }
    }
}

impl Error for SqlError {}

impl SqlError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingField { .. } => ErrorCode::SqlMissingField,
            Self::Unsupported { .. } => ErrorCode::SqlUnsupported,
        }
    }
}

impl DiagnosticError for SqlError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.sql"
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::diagnostics::{DiagnosticContext, DiagnosticError, ErrorCode};

    use super::SqlError;

    #[test]
    fn sql_error_exposes_stable_code_and_event_shape() {
        let error = SqlError::Unsupported {
            operation: "merge_into",
            reason: "not enabled in phase 1".to_owned(),
        };
        let event = error.to_log_event("intent.rejected", DiagnosticContext::default());

        assert_eq!(error.error_code(), ErrorCode::SqlUnsupported);
        assert_eq!(event.target, "brewdb.sql");
        assert_eq!(event.error_code, Some(ErrorCode::SqlUnsupported));
        assert_eq!(event.event_name, "intent.rejected");
    }
}
