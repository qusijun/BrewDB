//! Runtime orchestration error surface.

use std::error::Error;
use std::fmt;

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
