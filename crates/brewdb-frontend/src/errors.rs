//! Frontend-facing error types.

use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};

const FRONTEND_AUTHENTICATION_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_FRONTEND_AUTHENTICATION_FAILED");
const FRONTEND_INVALID_REQUEST: ErrorCode = ErrorCode::new("BREWDB_FRONTEND_INVALID_REQUEST");
const FRONTEND_SESSION_NOT_FOUND: ErrorCode = ErrorCode::new("BREWDB_FRONTEND_SESSION_NOT_FOUND");
const FRONTEND_UNSUPPORTED_PROTOCOL_MESSAGE: ErrorCode =
    ErrorCode::new("BREWDB_FRONTEND_UNSUPPORTED_PROTOCOL_MESSAGE");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontendError {
    AuthenticationFailed { reason: String },
    InvalidRequest { reason: String },
    SessionNotFound { session_id: String },
    UnsupportedProtocolMessage { message: String },
}

impl fmt::Display for FrontendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthenticationFailed { reason } => {
                write!(f, "authentication failed: {reason}")
            }
            Self::InvalidRequest { reason } => write!(f, "invalid client request: {reason}"),
            Self::SessionNotFound { session_id } => {
                write!(f, "client session `{session_id}` was not found")
            }
            Self::UnsupportedProtocolMessage { message } => {
                write!(f, "unsupported protocol message: {message}")
            }
        }
    }
}

impl Error for FrontendError {}

impl DiagnosticError for FrontendError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::AuthenticationFailed { .. } => FRONTEND_AUTHENTICATION_FAILED,
            Self::InvalidRequest { .. } => FRONTEND_INVALID_REQUEST,
            Self::SessionNotFound { .. } => FRONTEND_SESSION_NOT_FOUND,
            Self::UnsupportedProtocolMessage { .. } => FRONTEND_UNSUPPORTED_PROTOCOL_MESSAGE,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.frontend"
    }
}
