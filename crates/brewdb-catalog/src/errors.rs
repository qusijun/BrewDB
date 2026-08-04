//! Stable catalog error surface.

use std::error::Error;
use std::fmt;

use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};
use brewdb_common::errors::CommonError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogError {
    Common(CommonError),
    InvalidPath {
        kind: &'static str,
        value: String,
    },
    CatalogNotFound {
        catalog: String,
    },
    CatalogRefNotFound {
        catalog_id: String,
    },
    DatabaseNotFound {
        catalog: String,
        database: String,
    },
    DatabaseRefNotFound {
        database_id: String,
    },
    TableNotFound {
        catalog: String,
        database: String,
        table: String,
    },
    TableRefNotFound {
        table_id: String,
    },
    CatalogNotRegistered {
        catalog: String,
    },
    CatalogBackend {
        backend: &'static str,
        message: String,
    },
    BackendNotImplemented {
        backend: &'static str,
    },
    UnsupportedCatalogOperation {
        operation: &'static str,
    },
    UnsupportedSchemaType {
        backend: &'static str,
        type_name: String,
    },
    CatalogFormatMismatch {
        catalog: String,
        expected: &'static str,
        actual: &'static str,
    },
    DuplicateCatalog {
        catalog: String,
    },
    DuplicateDatabase {
        catalog: String,
        database: String,
    },
    DuplicateTable {
        catalog: String,
        database: String,
        table: String,
    },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Common(error) => write!(f, "{error}"),
            Self::InvalidPath { kind, value } => {
                write!(f, "invalid {kind} path segment: `{value}`")
            }
            Self::CatalogNotFound { catalog } => write!(f, "catalog not found: `{catalog}`"),
            Self::CatalogRefNotFound { catalog_id } => {
                write!(f, "catalog not found for ref: `{catalog_id}`")
            }
            Self::DatabaseNotFound { catalog, database } => {
                write!(f, "database not found: `{}.{}`", catalog, database)
            }
            Self::DatabaseRefNotFound { database_id } => {
                write!(f, "database not found for ref: `{database_id}`")
            }
            Self::TableNotFound {
                catalog,
                database,
                table,
            } => write!(f, "table not found: `{}.{}.{}`", catalog, database, table),
            Self::TableRefNotFound { table_id } => {
                write!(f, "table not found for ref: `{table_id}`")
            }
            Self::CatalogNotRegistered { catalog } => {
                write!(f, "catalog implementation is not registered: `{catalog}`")
            }
            Self::CatalogBackend { backend, message } => {
                write!(f, "catalog backend `{backend}` error: {message}")
            }
            Self::BackendNotImplemented { backend } => {
                write!(f, "catalog store backend not implemented: `{backend}`")
            }
            Self::UnsupportedCatalogOperation { operation } => {
                write!(f, "catalog operation is not supported: `{operation}`")
            }
            Self::UnsupportedSchemaType { backend, type_name } => {
                write!(f, "unsupported schema type from `{backend}`: `{type_name}`")
            }
            Self::CatalogFormatMismatch {
                catalog,
                expected,
                actual,
            } => write!(
                f,
                "table format mismatch for catalog `{catalog}`: expected `{expected}`, got `{actual}`"
            ),
            Self::DuplicateCatalog { catalog } => write!(f, "catalog already exists: `{catalog}`"),
            Self::DuplicateDatabase { catalog, database } => {
                write!(f, "database already exists: `{}.{}`", catalog, database)
            }
            Self::DuplicateTable {
                catalog,
                database,
                table,
            } => write!(
                f,
                "table already exists: `{}.{}.{}`",
                catalog, database, table
            ),
        }
    }
}

impl Error for CatalogError {}

impl DiagnosticError for CatalogError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::Common(error) => error.error_code(),
            Self::InvalidPath { .. } => ErrorCode::InvalidConfiguration,
            Self::CatalogNotFound { .. }
            | Self::CatalogNotRegistered { .. }
            | Self::CatalogRefNotFound { .. }
            | Self::DatabaseNotFound { .. }
            | Self::DatabaseRefNotFound { .. }
            | Self::TableNotFound { .. }
            | Self::TableRefNotFound { .. } => ErrorCode::NotFound,
            Self::CatalogBackend { .. } => ErrorCode::Internal,
            Self::BackendNotImplemented { .. }
            | Self::UnsupportedCatalogOperation { .. }
            | Self::UnsupportedSchemaType { .. } => ErrorCode::NotImplemented,
            Self::CatalogFormatMismatch { .. } => ErrorCode::InvalidConfiguration,
            Self::DuplicateCatalog { .. }
            | Self::DuplicateDatabase { .. }
            | Self::DuplicateTable { .. } => ErrorCode::AlreadyExists,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.catalog"
    }

    fn diagnostic_context(
        &self,
        event_name: &'static str,
    ) -> brewdb_common::diagnostics::DiagnosticContext {
        brewdb_common::diagnostics::DiagnosticContext::new(self.log_target(), event_name)
            .with_error_code(self.error_code())
            .with_error_variant(self.variant_name())
    }
}

impl From<CommonError> for CatalogError {
    fn from(value: CommonError) -> Self {
        Self::Common(value)
    }
}

impl CatalogError {
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::Common(_) => "Common",
            Self::InvalidPath { .. } => "InvalidPath",
            Self::CatalogNotFound { .. } => "CatalogNotFound",
            Self::CatalogRefNotFound { .. } => "CatalogRefNotFound",
            Self::DatabaseNotFound { .. } => "DatabaseNotFound",
            Self::DatabaseRefNotFound { .. } => "DatabaseRefNotFound",
            Self::TableNotFound { .. } => "TableNotFound",
            Self::TableRefNotFound { .. } => "TableRefNotFound",
            Self::CatalogNotRegistered { .. } => "CatalogNotRegistered",
            Self::CatalogBackend { .. } => "CatalogBackend",
            Self::BackendNotImplemented { .. } => "BackendNotImplemented",
            Self::UnsupportedCatalogOperation { .. } => "UnsupportedCatalogOperation",
            Self::UnsupportedSchemaType { .. } => "UnsupportedSchemaType",
            Self::CatalogFormatMismatch { .. } => "CatalogFormatMismatch",
            Self::DuplicateCatalog { .. } => "DuplicateCatalog",
            Self::DuplicateDatabase { .. } => "DuplicateDatabase",
            Self::DuplicateTable { .. } => "DuplicateTable",
        }
    }
}

#[cfg(test)]
mod tests {
    use brewdb_common::diagnostics::{DiagnosticError, ErrorCode};

    use super::CatalogError;

    #[test]
    fn catalog_error_diagnostic_context_includes_variant_name() {
        let error = CatalogError::TableNotFound {
            catalog: "prod".to_owned(),
            database: "sales".to_owned(),
            table: "orders".to_owned(),
        };

        let context = error.diagnostic_context("catalog.resolve_table");

        assert_eq!(context.target, "brewdb.catalog");
        assert_eq!(context.event_name, "catalog.resolve_table");
        assert_eq!(context.error_code, Some(ErrorCode::NotFound));
        assert_eq!(context.error_variant, Some("TableNotFound"));
    }
}
