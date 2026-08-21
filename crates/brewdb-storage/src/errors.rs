//! Stable storage error surface.

use std::error::Error;
use std::fmt;

use crate::common::diagnostics::{DiagnosticError, ErrorCode};

const STORAGE_REGISTRY_INVALID: ErrorCode = ErrorCode::new("BREWDB_STORAGE_REGISTRY_INVALID");
const STORAGE_TABLE_NOT_FOUND: ErrorCode = ErrorCode::new("BREWDB_STORAGE_TABLE_NOT_FOUND");
const STORAGE_UNSUPPORTED_STORAGE_KIND: ErrorCode =
    ErrorCode::new("BREWDB_STORAGE_UNSUPPORTED_STORAGE_KIND");
const STORAGE_TABLE_SCAN_FAILED: ErrorCode = ErrorCode::new("BREWDB_STORAGE_TABLE_SCAN_FAILED");

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    StorageRegistryInvalid { reason: String },
    TableNotFound { table_id: uuid::Uuid },
    UnsupportedStorageKind { storage_kind: String },
    TableScanFailed { reason: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageRegistryInvalid { reason } => {
                write!(f, "storage registry invalid: {reason}")
            }
            Self::TableNotFound { table_id } => write!(f, "table not found: {table_id}"),
            Self::UnsupportedStorageKind { storage_kind } => {
                write!(f, "unsupported storage kind: {storage_kind}")
            }
            Self::TableScanFailed { reason } => write!(f, "table scan failed: {reason}"),
        }
    }
}

impl Error for StorageError {}

impl DiagnosticError for StorageError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::StorageRegistryInvalid { .. } => STORAGE_REGISTRY_INVALID,
            Self::TableNotFound { .. } => STORAGE_TABLE_NOT_FOUND,
            Self::UnsupportedStorageKind { .. } => STORAGE_UNSUPPORTED_STORAGE_KIND,
            Self::TableScanFailed { .. } => STORAGE_TABLE_SCAN_FAILED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.storage"
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

impl StorageError {
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::StorageRegistryInvalid { .. } => "StorageRegistryInvalid",
            Self::TableNotFound { .. } => "TableNotFound",
            Self::UnsupportedStorageKind { .. } => "UnsupportedStorageKind",
            Self::TableScanFailed { .. } => "TableScanFailed",
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::diagnostics::DiagnosticError;

    use super::StorageError;

    #[test]
    fn storage_error_diagnostic_context_includes_variant_name() {
        let error = StorageError::UnsupportedStorageKind {
            storage_kind: "iceberg".to_owned(),
        };

        let context = error.diagnostic_context("storage.open_table");

        assert_eq!(context.target, "brewdb.storage");
        assert_eq!(
            context.error_code_str(),
            Some("BREWDB_STORAGE_UNSUPPORTED_STORAGE_KIND")
        );
        assert_eq!(context.error_variant, Some("UnsupportedStorageKind"));
    }
}
