//! Planner error surface.

use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};

const PLANNER_INVALID_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_INVALID_PLAN");
const PLANNER_UNSUPPORTED_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_UNSUPPORTED_PLAN");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    InvalidPlan { reason: String },
    UnsupportedPlan { reason: String },
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid planner input: {reason}"),
            Self::UnsupportedPlan { reason } => {
                write!(f, "unsupported distributed planning shape: {reason}")
            }
        }
    }
}

impl Error for PlannerError {}

impl DiagnosticError for PlannerError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidPlan { .. } => PLANNER_INVALID_PLAN,
            Self::UnsupportedPlan { .. } => PLANNER_UNSUPPORTED_PLAN,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.planner"
    }

    fn diagnostic_context(
        &self,
        event_name: &'static str,
    ) -> brewdb_common::diagnostics::DiagnosticContext {
        brewdb_common::diagnostics::DiagnosticContext::new(self.log_target(), event_name)
            .with_error_code(self.error_code())
    }
}
