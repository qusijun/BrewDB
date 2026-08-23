//! Worker execution error surfaces.

use std::error::Error;
use std::fmt;

use crate::catalog::CatalogError;
use crate::common::diagnostics::{DiagnosticError, ErrorCode};
use crate::parser::parser::ParserError;
use crate::planner::PlannerError;
use crate::planner::distributed::PlanFragmentId;
use crate::runtime::exchange::{ExchangeDataEncoding, ExchangeId};

const SQL_DRIVER_INVALID_REQUEST: ErrorCode = ErrorCode::new("BREWDB_SQL_DRIVER_INVALID_REQUEST");
const SQL_DRIVER_UNSUPPORTED_STATEMENT: ErrorCode =
    ErrorCode::new("BREWDB_SQL_DRIVER_UNSUPPORTED_STATEMENT");
const SCHEDULER_EMPTY_PLAN: ErrorCode = ErrorCode::new("BREWDB_RUNTIME_SCHEDULER_EMPTY_PLAN");
const SCHEDULER_NO_AVAILABLE_WORKER: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_SCHEDULER_NO_AVAILABLE_WORKER");
const EXCHANGE_SOURCE_FRAGMENT_NOT_SCHEDULED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_SOURCE_FRAGMENT_NOT_SCHEDULED");
const EXCHANGE_TARGET_FRAGMENT_NOT_SCHEDULED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_TARGET_FRAGMENT_NOT_SCHEDULED");
const EXCHANGE_RECEIVER_ALREADY_TAKEN: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_RECEIVER_ALREADY_TAKEN");
const EXCHANGE_RECEIVER_CLOSED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_RECEIVER_CLOSED");
const EXCHANGE_EMPTY_OUTPUTS: ErrorCode = ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_EMPTY_OUTPUTS");
const EXCHANGE_INVALID_ROUTING: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_INVALID_ROUTING");
const EXCHANGE_IPC_SERIALIZATION_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_IPC_SERIALIZATION_FAILED");
const EXCHANGE_UNSUPPORTED_DATA_ENCODING: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_UNSUPPORTED_DATA_ENCODING");
const EXCHANGE_BUFFER_LOCK_POISONED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXCHANGE_BUFFER_LOCK_POISONED");
const EXECUTION_INVALID_PLAN: ErrorCode = ErrorCode::new("BREWDB_RUNTIME_EXECUTION_INVALID_PLAN");
const EXECUTION_RUNTIME_INIT_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_EXECUTION_RUNTIME_INIT_FAILED");
const EXECUTION_STORAGE_ERROR: ErrorCode = ErrorCode::new("BREWDB_RUNTIME_EXECUTION_STORAGE_ERROR");
const EXECUTION_CATALOG_ERROR: ErrorCode = ErrorCode::new("BREWDB_RUNTIME_EXECUTION_CATALOG_ERROR");
const TRANSPORT_ENDPOINT_NOT_FOUND: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_TRANSPORT_ENDPOINT_NOT_FOUND");
const TRANSPORT_EXECUTION_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_RUNTIME_TRANSPORT_EXECUTION_FAILED");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangeRuntimeError {
    SourceFragmentNotScheduled { fragment_id: PlanFragmentId },
    TargetFragmentNotScheduled { fragment_id: PlanFragmentId },
    ExchangeReceiverAlreadyTaken { exchange_id: ExchangeId },
    ExchangeReceiverClosed { exchange_id: ExchangeId },
    EmptyExchangeOutputs,
    InvalidExchangeRouting { reason: String },
    IpcSerializationFailed { reason: String },
    UnsupportedDataEncoding { encoding: ExchangeDataEncoding },
    BufferLockPoisoned,
}

impl fmt::Display for ExchangeRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceFragmentNotScheduled { fragment_id } => {
                write!(
                    f,
                    "exchange source fragment {:?} is not scheduled",
                    fragment_id
                )
            }
            Self::TargetFragmentNotScheduled { fragment_id } => {
                write!(
                    f,
                    "exchange target fragment {:?} is not scheduled",
                    fragment_id
                )
            }
            Self::ExchangeReceiverAlreadyTaken { exchange_id } => {
                write!(f, "exchange receiver already taken for {:?}", exchange_id)
            }
            Self::ExchangeReceiverClosed { exchange_id } => {
                write!(f, "exchange receiver is closed for {:?}", exchange_id)
            }
            Self::EmptyExchangeOutputs => write!(f, "exchange output channels are empty"),
            Self::InvalidExchangeRouting { reason } => {
                write!(f, "invalid exchange routing: {reason}")
            }
            Self::IpcSerializationFailed { reason } => {
                write!(f, "exchange Arrow IPC serialization failed: {reason}")
            }
            Self::UnsupportedDataEncoding { encoding } => {
                write!(f, "unsupported exchange data encoding: {encoding:?}")
            }
            Self::BufferLockPoisoned => write!(f, "exchange buffer lock is poisoned"),
        }
    }
}

impl Error for ExchangeRuntimeError {}

impl DiagnosticError for ExchangeRuntimeError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::SourceFragmentNotScheduled { .. } => EXCHANGE_SOURCE_FRAGMENT_NOT_SCHEDULED,
            Self::TargetFragmentNotScheduled { .. } => EXCHANGE_TARGET_FRAGMENT_NOT_SCHEDULED,
            Self::ExchangeReceiverAlreadyTaken { .. } => EXCHANGE_RECEIVER_ALREADY_TAKEN,
            Self::ExchangeReceiverClosed { .. } => EXCHANGE_RECEIVER_CLOSED,
            Self::EmptyExchangeOutputs => EXCHANGE_EMPTY_OUTPUTS,
            Self::InvalidExchangeRouting { .. } => EXCHANGE_INVALID_ROUTING,
            Self::IpcSerializationFailed { .. } => EXCHANGE_IPC_SERIALIZATION_FAILED,
            Self::UnsupportedDataEncoding { .. } => EXCHANGE_UNSUPPORTED_DATA_ENCODING,
            Self::BufferLockPoisoned => EXCHANGE_BUFFER_LOCK_POISONED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentSchedulerError {
    EmptyPlan,
    NoAvailableWorker,
}

impl fmt::Display for FragmentSchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPlan => write!(f, "distributed plan has no fragments"),
            Self::NoAvailableWorker => write!(f, "no available workers"),
        }
    }
}

impl Error for FragmentSchedulerError {}

impl DiagnosticError for FragmentSchedulerError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::EmptyPlan => SCHEDULER_EMPTY_PLAN,
            Self::NoAvailableWorker => SCHEDULER_NO_AVAILABLE_WORKER,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionRuntimeError {
    InvalidPlan { reason: String },
    RuntimeInitFailed { reason: String },
    StorageError { reason: String },
    CatalogError { reason: String },
}

impl fmt::Display for ExecutionRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid execution plan: {reason}"),
            Self::RuntimeInitFailed { reason } => {
                write!(f, "runtime initialization failed: {reason}")
            }
            Self::StorageError { reason } => write!(f, "storage error: {reason}"),
            Self::CatalogError { reason } => write!(f, "catalog error: {reason}"),
        }
    }
}

impl Error for ExecutionRuntimeError {}

impl DiagnosticError for ExecutionRuntimeError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidPlan { .. } => EXECUTION_INVALID_PLAN,
            Self::RuntimeInitFailed { .. } => EXECUTION_RUNTIME_INIT_FAILED,
            Self::StorageError { .. } => EXECUTION_STORAGE_ERROR,
            Self::CatalogError { .. } => EXECUTION_CATALOG_ERROR,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcError {
    EndpointNotFound { endpoint: String },
    ExecutionFailed { reason: String },
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndpointNotFound { endpoint } => write!(f, "rpc endpoint not found: {endpoint}"),
            Self::ExecutionFailed { reason } => write!(f, "rpc execution failed: {reason}"),
        }
    }
}

impl Error for RpcError {}

impl DiagnosticError for RpcError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::EndpointNotFound { .. } => TRANSPORT_ENDPOINT_NOT_FOUND,
            Self::ExecutionFailed { .. } => TRANSPORT_EXECUTION_FAILED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

#[derive(Debug)]
pub enum SqlDriverError {
    Catalog(CatalogError),
    Parser(ParserError),
    InvalidRequest { reason: String },
    Planner(PlannerError),
    Runtime(ExecutionRuntimeError),
    UnsupportedStatement { reason: String },
}

impl fmt::Display for SqlDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(err) => write!(f, "{err}"),
            Self::Parser(err) => write!(f, "sql parse failed: {err}"),
            Self::InvalidRequest { reason } => write!(f, "invalid sql driver request: {reason}"),
            Self::Planner(err) => write!(f, "{err}"),
            Self::Runtime(err) => write!(f, "{err}"),
            Self::UnsupportedStatement { reason } => {
                write!(
                    f,
                    "statement is not supported by the SQL query driver: {reason}"
                )
            }
        }
    }
}

impl Error for SqlDriverError {}

impl DiagnosticError for SqlDriverError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::Catalog(error) => error.error_code(),
            Self::Parser(error) => error.error_code(),
            Self::InvalidRequest { .. } => SQL_DRIVER_INVALID_REQUEST,
            Self::Planner(error) => error.error_code(),
            Self::Runtime(error) => error.error_code(),
            Self::UnsupportedStatement { .. } => SQL_DRIVER_UNSUPPORTED_STATEMENT,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

impl From<CatalogError> for SqlDriverError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<ParserError> for SqlDriverError {
    fn from(value: ParserError) -> Self {
        Self::Parser(value)
    }
}

impl From<PlannerError> for SqlDriverError {
    fn from(value: PlannerError) -> Self {
        Self::Planner(value)
    }
}

impl From<ExecutionRuntimeError> for SqlDriverError {
    fn from(value: ExecutionRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::diagnostics::DiagnosticError;
    use crate::planner::distributed::PlanFragmentId;
    use crate::runtime::exchange::ExchangeId;

    use crate::parser::parser::ParserError;

    use super::{ExchangeRuntimeError, ExecutionRuntimeError, FragmentSchedulerError, RpcError};

    #[test]
    fn scheduler_error_uses_runtime_diagnostic_code() {
        let error = FragmentSchedulerError::NoAvailableWorker;

        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_RUNTIME_SCHEDULER_NO_AVAILABLE_WORKER"
        );
        assert_eq!(error.log_target(), "brewdb.runtime");
    }

    #[test]
    fn exchange_error_uses_runtime_diagnostic_code() {
        let error = ExchangeRuntimeError::ExchangeReceiverAlreadyTaken {
            exchange_id: ExchangeId(7),
        };

        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_RUNTIME_EXCHANGE_RECEIVER_ALREADY_TAKEN"
        );
        assert_eq!(error.log_target(), "brewdb.runtime");
    }

    #[test]
    fn exchange_fragment_schedule_error_uses_specific_code() {
        let error = ExchangeRuntimeError::SourceFragmentNotScheduled {
            fragment_id: PlanFragmentId(1),
        };

        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_RUNTIME_EXCHANGE_SOURCE_FRAGMENT_NOT_SCHEDULED"
        );
    }

    #[test]
    fn execution_error_uses_runtime_diagnostic_code() {
        let error = ExecutionRuntimeError::StorageError {
            reason: "missing table".to_owned(),
        };

        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_RUNTIME_EXECUTION_STORAGE_ERROR"
        );
        assert_eq!(error.log_target(), "brewdb.runtime");
    }

    #[test]
    fn rpc_error_uses_runtime_transport_diagnostic_code() {
        let missing_endpoint = RpcError::EndpointNotFound {
            endpoint: "rpc://worker-404".to_owned(),
        };
        let execution_failed = RpcError::ExecutionFailed {
            reason: "worker rejected fragment".to_owned(),
        };

        assert_eq!(
            missing_endpoint.error_code().as_str(),
            "BREWDB_RUNTIME_TRANSPORT_ENDPOINT_NOT_FOUND"
        );
        assert_eq!(
            execution_failed.error_code().as_str(),
            "BREWDB_RUNTIME_TRANSPORT_EXECUTION_FAILED"
        );
        assert_eq!(missing_endpoint.log_target(), "brewdb.runtime");
        assert_eq!(execution_failed.log_target(), "brewdb.runtime");
    }

    #[test]
    fn sql_driver_runtime_error_keeps_execution_error_code() {
        let error = super::SqlDriverError::Runtime(ExecutionRuntimeError::InvalidPlan {
            reason: "missing root".to_owned(),
        });

        assert_eq!(
            error.error_code().as_str(),
            "BREWDB_RUNTIME_EXECUTION_INVALID_PLAN"
        );
    }

    #[test]
    fn sql_driver_parser_error_keeps_parser_error_code() {
        let error = super::SqlDriverError::Parser(ParserError::ParserError(
            "expected statement".to_owned(),
        ));

        assert_eq!(error.error_code().as_str(), "BREWDB_PARSER_PARSE_ERROR");
    }
}
