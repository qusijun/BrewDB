//! Shared error categories used across BrewDB crates.

use std::error::Error;
use std::fmt;

use crate::diagnostics::{DiagnosticError, ErrorCode};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoreError {
    InvalidStateTransition {
        entity: &'static str,
        from: &'static str,
        to: &'static str,
    },
    InvalidIdentifier {
        entity: &'static str,
        reason: String,
    },
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStateTransition { entity, from, to } => {
                write!(f, "invalid state transition for {entity}: {from} -> {to}")
            }
            Self::InvalidIdentifier { entity, reason } => {
                write!(f, "invalid identifier for {entity}: {reason}")
            }
        }
    }
}

impl Error for CoreError {}

impl CoreError {
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidStateTransition { .. } => ErrorCode::CoreInvalidStateTransition,
            Self::InvalidIdentifier { .. } => ErrorCode::CoreInvalidIdentifier,
        }
    }
}

impl DiagnosticError for CoreError {
    fn error_code(&self) -> ErrorCode {
        self.error_code()
    }

    fn log_target(&self) -> &'static str {
        "brewdb.core"
    }
}
