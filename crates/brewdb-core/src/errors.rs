//! Shared error categories used across BrewDB crates.

use std::error::Error;
use std::fmt;

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
