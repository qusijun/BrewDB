//! Shared foundational error helpers.

use std::error::Error;
use std::fmt;

use crate::common::diagnostics::{DiagnosticError, ErrorCode};
use arrow::error::ArrowError;
use datafusion_common::{DataFusionError, SchemaError};

const COMMON_INVALID_CONFIGURATION: ErrorCode = ErrorCode::INVALID_CONFIGURATION;
const COMMON_LOGGING_INITIALIZATION_FAILED: ErrorCode = ErrorCode::LOGGING_INITIALIZATION_FAILED;
const COMMON_SCHEMA_CONVERSION_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_COMMON_SCHEMA_CONVERSION_FAILED");
const COMMON_INVALID_TABLE_REFERENCE: ErrorCode =
    ErrorCode::new("BREWDB_COMMON_INVALID_TABLE_REFERENCE");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommonError {
    InvalidConfiguration { field: String, reason: String },
    LoggingInitializationFailed { reason: String },
    SchemaConversionFailed { reason: String },
    InvalidTableReference { reference: String },
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration { field, reason } => {
                write!(f, "invalid configuration for `{field}`: {reason}")
            }
            Self::LoggingInitializationFailed { reason } => {
                write!(f, "logging initialization failed: {reason}")
            }
            Self::SchemaConversionFailed { reason } => {
                write!(f, "schema conversion failed: {reason}")
            }
            Self::InvalidTableReference { reference } => {
                write!(f, "table reference must be fully qualified: {reference}")
            }
        }
    }
}

impl Error for CommonError {}

impl DiagnosticError for CommonError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidConfiguration { .. } => COMMON_INVALID_CONFIGURATION,
            Self::LoggingInitializationFailed { .. } => COMMON_LOGGING_INITIALIZATION_FAILED,
            Self::SchemaConversionFailed { .. } => COMMON_SCHEMA_CONVERSION_FAILED,
            Self::InvalidTableReference { .. } => COMMON_INVALID_TABLE_REFERENCE,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.common"
    }
}

#[allow(unreachable_patterns)]
pub fn datafusion_error_variant_name(error: &DataFusionError) -> &'static str {
    match error {
        DataFusionError::ArrowError(error, _) => arrow_error_variant_name(error.as_ref()),
        DataFusionError::IoError(_) => "IoError",
        DataFusionError::NotImplemented(_) => "NotImplemented",
        DataFusionError::Internal(_) => "Internal",
        DataFusionError::Plan(_) => "Plan",
        DataFusionError::Configuration(_) => "Configuration",
        DataFusionError::SchemaError(error, _) => schema_error_variant_name(error.as_ref()),
        DataFusionError::Execution(_) | DataFusionError::ExecutionJoin(_) => "Execution",
        DataFusionError::ResourcesExhausted(_) => "ResourcesExhausted",
        DataFusionError::External(_) => "External",
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_error_variant_name(error)
        }
        DataFusionError::Substrait(_) => "Substrait",
        DataFusionError::Collection(_) => "Collection",
        DataFusionError::Shared(error) => datafusion_error_variant_name(error.as_ref()),
        DataFusionError::Ffi(_) => "Ffi",
        _ => "External",
    }
}

pub fn datafusion_error_is_data_read(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::ArrowError(error, _) => arrow_error_is_data_read(error.as_ref()),
        DataFusionError::IoError(_) => true,
        DataFusionError::Execution(reason) => datafusion_error_message_is_data_read(reason),
        DataFusionError::External(error) => {
            datafusion_error_message_is_data_read(&error.to_string())
        }
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_error_is_data_read(error)
        }
        DataFusionError::Shared(error) => datafusion_error_is_data_read(error.as_ref()),
        _ => false,
    }
}

pub fn datafusion_error_message_is_data_read(message: &str) -> bool {
    message.contains("Csv error:")
        || message.contains("Parquet error:")
        || message.contains("Object Store error:")
}

fn schema_error_variant_name(error: &SchemaError) -> &'static str {
    match error {
        SchemaError::AmbiguousReference { .. } => "AmbiguousReference",
        SchemaError::DuplicateQualifiedField { .. } => "DuplicateQualifiedField",
        SchemaError::DuplicateUnqualifiedField { .. } => "DuplicateUnqualifiedField",
        SchemaError::FieldNotFound { .. } => "FieldNotFound",
    }
}

fn arrow_error_variant_name(error: &ArrowError) -> &'static str {
    match error {
        ArrowError::NotYetImplemented(_) => "NotYetImplemented",
        ArrowError::ExternalError(_) => "ExternalError",
        ArrowError::CastError(_) => "CastError",
        ArrowError::MemoryError(_) => "MemoryError",
        ArrowError::ParseError(_) => "ParseError",
        ArrowError::SchemaError(_) => "SchemaError",
        ArrowError::ComputeError(_) => "ComputeError",
        ArrowError::DivideByZero => "DivideByZero",
        ArrowError::ArithmeticOverflow(_) => "ArithmeticOverflow",
        ArrowError::CsvError(_) => "CsvError",
        ArrowError::JsonError(_) => "JsonError",
        ArrowError::AvroError(_) => "AvroError",
        ArrowError::IoError(_, _) => "IoError",
        ArrowError::IpcError(_) => "IpcError",
        ArrowError::InvalidArgumentError(_) => "InvalidArgumentError",
        ArrowError::ParquetError(_) => "ParquetError",
        ArrowError::CDataInterface(_) => "CDataInterface",
        ArrowError::DictionaryKeyOverflowError => "DictionaryKeyOverflowError",
        ArrowError::RunEndIndexOverflowError => "RunEndIndexOverflowError",
        ArrowError::OffsetOverflowError(_) => "OffsetOverflowError",
    }
}

fn arrow_error_is_data_read(error: &ArrowError) -> bool {
    match error {
        ArrowError::CsvError(_)
        | ArrowError::JsonError(_)
        | ArrowError::AvroError(_)
        | ArrowError::IoError(_, _)
        | ArrowError::ParquetError(_) => true,
        ArrowError::ExternalError(error) => {
            datafusion_error_message_is_data_read(&error.to_string())
        }
        _ => false,
    }
}

#[cfg(test)]
mod datafusion_tests {
    use super::{datafusion_error_is_data_read, datafusion_error_variant_name};
    use arrow::error::ArrowError;
    use datafusion_common::DataFusionError;

    #[test]
    fn classifies_datafusion_plan_error() {
        let error = DataFusionError::Plan("bad projection".to_owned());

        assert_eq!(datafusion_error_variant_name(&error), "Plan");
    }

    #[test]
    fn classifies_arrow_csv_error_as_data_read() {
        let error = DataFusionError::ArrowError(
            Box::new(ArrowError::CsvError(
                "incorrect number of fields".to_owned(),
            )),
            None,
        );

        assert!(datafusion_error_is_data_read(&error));
    }

    #[test]
    fn classifies_wrapped_csv_execution_error_as_data_read() {
        let error = DataFusionError::Execution(
            "Arrow error: Csv error: incorrect number of fields".to_owned(),
        );

        assert!(datafusion_error_is_data_read(&error));
    }
}
