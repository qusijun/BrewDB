//! Planner error surface.

use std::error::Error;
use std::fmt;

use crate::common::diagnostics::{DiagnosticError, ErrorCode};
use crate::common::errors::{datafusion_error_is_data_read, datafusion_error_variant_name};
use datafusion_common::DataFusionError;

const PLANNER_INVALID_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_INVALID_PLAN");
const PLANNER_UNSUPPORTED_PLAN: ErrorCode = ErrorCode::new("BREWDB_PLANNER_UNSUPPORTED_PLAN");
const PLANNER_PLAN_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_PLAN_ERROR");
const PLANNER_SCHEMA_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_SCHEMA_ERROR");
const PLANNER_INTERNAL_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_INTERNAL_ERROR");
const PLANNER_EXECUTION_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_EXECUTION_ERROR");
const PLANNER_EXTERNAL_ERROR: ErrorCode = ErrorCode::new("BREWDB_PLANNER_EXTERNAL_ERROR");
const PLANNER_NOT_IMPLEMENTED: ErrorCode = ErrorCode::new("BREWDB_PLANNER_NOT_IMPLEMENTED");

#[derive(Debug)]
pub enum PlannerError {
    InvalidPlan {
        reason: String,
    },
    UnsupportedPlan {
        reason: String,
    },
    Plan {
        reason: String,
        cause: Option<DataFusionError>,
    },
    Schema {
        reason: String,
        cause: Option<DataFusionError>,
    },
    Internal {
        reason: String,
        cause: Option<DataFusionError>,
    },
    Execution {
        reason: String,
        cause: Option<DataFusionError>,
    },
    External {
        reason: String,
        cause: Option<DataFusionError>,
    },
    NotImplemented {
        reason: String,
        cause: Option<DataFusionError>,
    },
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid planner input: {reason}"),
            Self::UnsupportedPlan { reason } => {
                write!(f, "unsupported planner shape: {reason}")
            }
            Self::Plan { reason, .. } => write!(f, "planner error: {reason}"),
            Self::Schema { reason, .. } => write!(f, "planner schema error: {reason}"),
            Self::Internal { reason, .. } => write!(f, "planner internal error: {reason}"),
            Self::Execution { reason, .. } => write!(f, "planner execution error: {reason}"),
            Self::External { reason, .. } => write!(f, "planner external error: {reason}"),
            Self::NotImplemented { reason, .. } => {
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
            .with_error_variant(self.variant_name())
    }
}

pub(crate) fn map_df_plan_error(error: DataFusionError) -> PlannerError {
    PlannerError::from(error)
}

impl From<DataFusionError> for PlannerError {
    fn from(error: DataFusionError) -> Self {
        map_datafusion_error(error)
    }
}

fn map_datafusion_error(error: DataFusionError) -> PlannerError {
    let reason = error.to_string();
    if datafusion_error_is_data_read(&error) {
        return PlannerError::Execution {
            reason,
            cause: Some(error),
        };
    }
    match datafusion_error_variant_name(&error) {
        "NotImplemented" => PlannerError::NotImplemented {
            reason,
            cause: Some(error),
        },
        "Internal" => PlannerError::Internal {
            reason,
            cause: Some(error),
        },
        "Plan" | "Configuration" => PlannerError::Plan {
            reason,
            cause: Some(error),
        },
        "AmbiguousReference"
        | "DuplicateQualifiedField"
        | "DuplicateUnqualifiedField"
        | "FieldNotFound"
        | "SchemaError" => PlannerError::Schema {
            reason,
            cause: Some(error),
        },
        "Execution" | "ResourcesExhausted" => PlannerError::Execution {
            reason,
            cause: Some(error),
        },
        _ => PlannerError::External {
            reason,
            cause: Some(error),
        },
    }
}

pub(crate) fn map_common_error(error: crate::common::errors::CommonError) -> PlannerError {
    use crate::common::errors::CommonError;

    match error {
        CommonError::SchemaConversionFailed { reason } => PlannerError::Schema {
            reason,
            cause: None,
        },
        CommonError::LoggingInitializationFailed { reason } => PlannerError::Internal {
            reason,
            cause: None,
        },
        CommonError::InvalidConfiguration { field, reason } => PlannerError::Plan {
            reason: format!("invalid configuration for `{field}`: {reason}"),
            cause: None,
        },
        CommonError::InvalidTableReference { reference } => PlannerError::Plan {
            reason: format!("table reference must be fully qualified: {reference}"),
            cause: None,
        },
    }
}

impl PlannerError {
    pub fn datafusion_cause(&self) -> Option<&DataFusionError> {
        match self {
            Self::Plan { cause, .. }
            | Self::Schema { cause, .. }
            | Self::Internal { cause, .. }
            | Self::Execution { cause, .. }
            | Self::External { cause, .. }
            | Self::NotImplemented { cause, .. } => cause.as_ref(),
            Self::InvalidPlan { .. } | Self::UnsupportedPlan { .. } => None,
        }
    }

    pub fn into_datafusion_cause(self) -> Option<DataFusionError> {
        match self {
            Self::Plan { cause, .. }
            | Self::Schema { cause, .. }
            | Self::Internal { cause, .. }
            | Self::Execution { cause, .. }
            | Self::External { cause, .. }
            | Self::NotImplemented { cause, .. } => cause,
            Self::InvalidPlan { .. } | Self::UnsupportedPlan { .. } => None,
        }
    }

    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::InvalidPlan { .. } => "InvalidPlan",
            Self::UnsupportedPlan { .. } => "UnsupportedPlan",
            Self::Plan { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("Plan"),
            Self::Schema { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("Schema"),
            Self::Internal { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("Internal"),
            Self::Execution { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("Execution"),
            Self::External { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("External"),
            Self::NotImplemented { cause, .. } => cause
                .as_ref()
                .map(datafusion_error_variant_name)
                .unwrap_or("NotImplemented"),
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
    fn planner_error_exposes_datafusion_cause() {
        let error = PlannerError::from(DataFusionError::Plan("bad projection".to_owned()));

        assert!(error.datafusion_cause().is_some());
        assert_eq!(error.variant_name(), "Plan");
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

        assert!(matches!(error, PlannerError::Schema { cause: Some(_), .. }));
        assert_eq!(error.error_code().as_str(), "BREWDB_PLANNER_SCHEMA_ERROR");
        assert_eq!(
            error.diagnostic_context("planner.bind").error_variant,
            Some("FieldNotFound")
        );
    }

    #[test]
    fn datafusion_internal_error_keeps_internal_error_code() {
        let error = map_df_plan_error(DataFusionError::Internal("bug".to_owned()));

        assert!(matches!(error, PlannerError::Internal { .. }));
        assert_eq!(error.error_code().as_str(), "BREWDB_PLANNER_INTERNAL_ERROR");
    }

    #[test]
    fn datafusion_error_from_impl_keeps_datafusion_cause() {
        let error = PlannerError::from(DataFusionError::Execution(
            "Arrow error: Csv error: incorrect number of fields".to_owned(),
        ));

        assert!(matches!(
            error,
            PlannerError::Execution { cause: Some(_), .. }
        ));
        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_PLANNER_EXECUTION_ERROR"
        );
    }

    #[test]
    fn datafusion_context_keeps_schema_detail() {
        let error = PlannerError::from(DataFusionError::Context(
            "while planning".to_owned(),
            Box::new(DataFusionError::SchemaError(
                Box::new(SchemaError::DuplicateUnqualifiedField {
                    name: "id".to_owned(),
                }),
                Box::new(None),
            )),
        ));

        assert!(matches!(error, PlannerError::Schema { cause: Some(_), .. }));
        assert_eq!(
            error.diagnostic_context("planner.bind").error_variant,
            Some("DuplicateUnqualifiedField")
        );
    }
}
