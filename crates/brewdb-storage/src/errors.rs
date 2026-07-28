//! Storage adapter error surface.

use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    MissingField {
        entity: &'static str,
        field: &'static str,
    },
    Unsupported {
        operation: &'static str,
        reason: String,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { entity, field } => {
                write!(f, "missing required field `{field}` for {entity}")
            }
            Self::Unsupported { operation, reason } => {
                write!(f, "unsupported storage operation `{operation}`: {reason}")
            }
        }
    }
}

impl Error for StorageError {}
