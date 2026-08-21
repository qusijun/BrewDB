//! Worker-side exchange execution contracts.
//!
//! This module owns the worker-facing abstraction for exchange data flow.
//! The transport layer may carry pages across process boundaries, but the
//! worker still needs a local service boundary for buffering, draining, and
//! eventually owning exchange-specific execution behavior.

use std::error::Error;
use std::fmt;

use crate::runtime::ExchangeRuntimeError;
use crate::runtime::exchange::{ExchangeDataPage, ExchangeId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerExchangeError {
    ExchangeRuntime { reason: String },
}

impl fmt::Display for WorkerExchangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExchangeRuntime { reason } => write!(f, "worker exchange failed: {reason}"),
        }
    }
}

impl Error for WorkerExchangeError {}

impl From<ExchangeRuntimeError> for WorkerExchangeError {
    fn from(value: ExchangeRuntimeError) -> Self {
        Self::ExchangeRuntime {
            reason: value.to_string(),
        }
    }
}

/// Worker-side exchange service.
///
/// Keep this trait focused on exchange lifecycle only. It should stay free of
/// transport concerns so local execution can own the logic directly and remote
/// transports can stay thin wrappers.
pub trait WorkerExchangeService: Send + Sync {
    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), WorkerExchangeError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, WorkerExchangeError>;
}
