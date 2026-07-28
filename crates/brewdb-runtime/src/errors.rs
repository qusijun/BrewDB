//! Runtime orchestration error surface.

use std::error::Error;
use std::fmt;

use brewdb_core::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    StateConflict {
        entity: &'static str,
        reason: String,
    },
    NotFound {
        entity: &'static str,
        id: String,
    },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::StateConflict { entity, reason } => {
                write!(f, "state conflict for {entity}: {reason}")
            }
            Self::NotFound { entity, id } => {
                write!(f, "{entity} not found: {id}")
            }
        }
    }
}

impl Error for RuntimeError {}

impl RuntimeError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::MissingField { .. } => ErrorCode::RuntimeMissingField,
            Self::StateConflict { .. } => ErrorCode::RuntimeStateConflict,
            Self::NotFound { .. } => ErrorCode::RuntimeNotFound,
        }
    }
}

impl DiagnosticError for RuntimeError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::diagnostics::{DiagnosticContext, DiagnosticError, ErrorCode};
    use brewdb_core::ids::JobId;

    use super::RuntimeError;

    #[test]
    fn runtime_error_exposes_stable_code_and_log_target() {
        let error = RuntimeError::StateConflict {
            entity: "dispatch",
            reason: "worker slots exhausted".to_owned(),
        };
        let event = error.to_log_event(
            "dispatch.failed",
            DiagnosticContext::default()
                .with_job_id(JobId::parse_str("550e8400-e29b-41d4-a716-446655441410").unwrap()),
        );

        assert_eq!(error.error_code(), ErrorCode::RuntimeStateConflict);
        assert_eq!(event.target, "brewdb.runtime");
        assert_eq!(event.error_code, Some(ErrorCode::RuntimeStateConflict));
        assert!(event.context.job_id.is_some());
    }
}
