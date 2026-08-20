//! Planner error surface.

use std::error::Error;
use std::fmt;

use crate::common::diagnostics::{DiagnosticError, ErrorCode};

const PLANNER_INVALID_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_INVALID_PLAN");
const PLANNER_UNSUPPORTED_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_UNSUPPORTED_PLAN");
const PLANNER_PLAN_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_PLAN_ERROR");
const PLANNER_SCHEMA_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_SCHEMA_ERROR");
const PLANNER_INTERNAL_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_INTERNAL_ERROR");
const PLANNER_EXECUTION_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_EXECUTION_ERROR");
const PLANNER_EXTERNAL_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_EXTERNAL_ERROR");
const PLANNER_NOT_IMPLEMENTED: ErrorCode = ErrorCode::new("BREWDB_PLANNER_NOT_IMPLEMENTED");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    InvalidPlan { reason: String },
    UnsupportedPlan { reason: String },
    Plan { reason: String },
    Schema { reason: String },
    Internal { reason: String },
    Execution { reason: String },
    External { reason: String },
    NotImplemented { reason: String },
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid planner input: {reason}"),
            Self::UnsupportedPlan { reason } => {
                write!(f, "unsupported planner shape: {reason}")
            }
            Self::Plan { reason } => write!(f, "planner error: {reason}"),
            Self::Schema { reason } => write!(f, "planner schema error: {reason}"),
            Self::Internal { reason } => write!(f, "planner internal error: {reason}"),
            Self::Execution { reason } => write!(f, "planner execution error: {reason}"),
            Self::External { reason } => write!(f, "planner external error: {reason}"),
            Self::NotImplemented { reason } => {
                write!(f, "planner feature not implemented: {reason}")
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
            Self::Plan { .. } => PLANNER_PLAN_ERROR,
            Self::Schema { .. } => PLANNER_SCHEMA_ERROR,
            Self::Internal { .. } => PLANNER_INTERNAL_ERROR,
            Self::Execution { .. } => PLANNER_EXECUTION_ERROR,
            Self::External { .. } => PLANNER_EXTERNAL_ERROR,
            Self::NotImplemented { .. } => PLANNER_NOT_IMPLEMENTED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.planner"
    }

    fn diagnostic_context(
        &self,
        event_name: &'static str,
    ) -> crate::common::diagnostics::DiagnosticContext {
        crate::common::diagnostics::DiagnosticContext::new(self.log_target(), event_name)
            .with_error_code(self.error_code())
    }
}

pub(crate) fn map_df_plan_error(error: datafusion_common::DataFusionError) -> PlannerError {
    use datafusion_common::DataFusionError;

    match error {
        DataFusionError::NotImplemented(reason) => PlannerError::NotImplemented { reason },
        DataFusionError::Internal(reason) => PlannerError::Internal { reason },
        DataFusionError::Plan(reason) => PlannerError::Plan { reason },
        DataFusionError::SchemaError(error, _) => PlannerError::Schema {
            reason: error.to_string(),
        },
        DataFusionError::Execution(reason) => PlannerError::Execution { reason },
        DataFusionError::ExecutionJoin(error) => PlannerError::Execution {
            reason: error.to_string(),
        },
        DataFusionError::ResourcesExhausted(reason) => PlannerError::Execution { reason },
        DataFusionError::External(error) => PlannerError::External {
            reason: error.to_string(),
        },
        DataFusionError::Context(context, error) => {
            let mapped = map_df_plan_error(*error);
            mapped.with_context(context)
        }
        DataFusionError::Diagnostic(_, error) => map_df_plan_error(*error),
        other => PlannerError::External {
            reason: other.to_string(),
        },
    }
}

pub(crate) fn map_common_error(error: crate::common::errors::CommonError) -> PlannerError {
    use crate::common::errors::CommonError;

    match error {
        CommonError::SchemaConversionFailed { reason } => PlannerError::Schema { reason },
        CommonError::LoggingInitializationFailed { reason } => PlannerError::Internal { reason },
        CommonError::InvalidConfiguration { field, reason } => PlannerError::Plan {
            reason: format!("invalid configuration for `{field}`: {reason}"),
        },
        CommonError::InvalidTableReference { reference } => PlannerError::Plan {
            reason: format!("table reference must be fully qualified: {reference}"),
        },
    }
}

impl PlannerError {
    fn with_context(self, context: String) -> Self {
        match self {
            Self::InvalidPlan { reason } => Self::InvalidPlan {
                reason: format!("{context}: {reason}"),
            },
            Self::UnsupportedPlan { reason } => Self::UnsupportedPlan {
                reason: format!("{context}: {reason}"),
            },
            Self::Plan { reason } => Self::Plan {
                reason: format!("{context}: {reason}"),
            },
            Self::Schema { reason } => Self::Schema {
                reason: format!("{context}: {reason}"),
            },
            Self::Internal { reason } => Self::Internal {
                reason: format!("{context}: {reason}"),
            },
            Self::Execution { reason } => Self::Execution {
                reason: format!("{context}: {reason}"),
            },
            Self::External { reason } => Self::External {
                reason: format!("{context}: {reason}"),
            },
            Self::NotImplemented { reason } => Self::NotImplemented {
                reason: format!("{context}: {reason}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::diagnostics::DiagnosticError;
    use datafusion_common::{Column, DataFusionError, SchemaError};

    use super::{map_df_plan_error, PlannerError};

    #[test]
    fn datafusion_plan_error_keeps_plan_error_code() {
        let error = map_df_plan_error(DataFusionError::Plan("bad projection".to_owned()));

        assert!(matches!(error, PlannerError::Plan { .. }));
        assert_eq!(error.error_code().as_str(), "BREWDB_PLANNER_PLAN_ERROR");
    }

    #[test]
    fn datafusion_schema_error_keeps_schema_error_code() {
        let error = map_df_plan_error(DataFusionError::SchemaError(
            Box::new(SchemaError::FieldNotFound {
                field: Box::new(Column::from_name("missing")),
                valid_fields: vec![Column::from_name("known")],
            }),
            Box::new(None),
        ));

        assert!(matches!(error, PlannerError::Schema { .. }));
        assert_eq!(error.error_code().as_str(), "BREWDB_PLANNER_SCHEMA_ERROR");
    }

    #[test]
    fn datafusion_internal_error_keeps_internal_error_code() {
        let error = map_df_plan_error(DataFusionError::Internal("bug".to_owned()));

        assert!(matches!(error, PlannerError::Internal { .. }));
        assert_eq!(error.error_code().as_str(), "BREWDB_PLANNER_INTERNAL_ERROR");
    }
}
