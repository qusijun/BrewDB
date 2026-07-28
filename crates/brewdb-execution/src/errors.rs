//! Execution-layer error surface.

use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    InvalidPlan {
        reason: String,
    },
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::InvalidPlan { reason } => write!(f, "invalid execution plan: {reason}"),
        }
    }
}

impl Error for ExecutionError {}
