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
    InvalidTableNameResolution {
        name: String,
        reason: String,
    },
    BackendNotImplemented {
        backend: &'static str,
    },
    UnsupportedCatalogOperation {
        operation: &'static str,
    },
    ConcurrentCatalogUpdate {
        object: String,
    },
    Backend {
        message: String,
    },
    Cache {
        message: String,
    },
    Normalization {
        message: String,
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
            Self::InvalidTableNameResolution { name, reason } => {
                write!(f, "failed to resolve table name `{name}`: {reason}")
            }
            Self::BackendNotImplemented { backend } => {
                write!(f, "catalog store backend not implemented: `{backend}`")
            }
            Self::UnsupportedCatalogOperation { operation } => {
                write!(f, "catalog operation is not supported: `{operation}`")
            }
            Self::ConcurrentCatalogUpdate { object } => {
                write!(f, "concurrent catalog update detected for `{object}`")
            }
            Self::Backend { message } => write!(f, "catalog backend error: {message}"),
            Self::Cache { message } => write!(f, "catalog cache error: {message}"),
            Self::Normalization { message } => {
                write!(f, "catalog normalization error: {message}")
            }
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
            Self::InvalidPath { .. } | Self::InvalidTableNameResolution { .. } => {
                ErrorCode::InvalidConfiguration
            }
            Self::CatalogNotFound { .. }
            | Self::CatalogRefNotFound { .. }
            | Self::DatabaseNotFound { .. }
            | Self::DatabaseRefNotFound { .. }
            | Self::TableNotFound { .. }
            | Self::TableRefNotFound { .. } => ErrorCode::NotFound,
            Self::BackendNotImplemented { .. } | Self::UnsupportedCatalogOperation { .. } => {
                ErrorCode::NotImplemented
            }
            Self::ConcurrentCatalogUpdate { .. }
            | Self::Backend { .. }
            | Self::Cache { .. }
            | Self::Normalization { .. } => ErrorCode::Internal,
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
            Self::InvalidTableNameResolution { .. } => "InvalidTableNameResolution",
            Self::BackendNotImplemented { .. } => "BackendNotImplemented",
            Self::UnsupportedCatalogOperation { .. } => "UnsupportedCatalogOperation",
            Self::ConcurrentCatalogUpdate { .. } => "ConcurrentCatalogUpdate",
            Self::Backend { .. } => "Backend",
            Self::Cache { .. } => "Cache",
            Self::Normalization { .. } => "Normalization",
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

    #[test]
    fn invalid_table_name_resolution_maps_to_invalid_configuration() {
        let error = CatalogError::InvalidTableNameResolution {
            name: "orders".to_owned(),
            reason: "default database is not set".to_owned(),
        };

        assert_eq!(error.error_code(), ErrorCode::InvalidConfiguration);
        assert_eq!(error.variant_name(), "InvalidTableNameResolution");
    }

    #[test]
    fn cache_and_normalization_errors_map_to_internal() {
        let cache = CatalogError::Cache {
            message: "cache refresh failed".to_owned(),
        };
        let normalization = CatalogError::Normalization {
            message: "missing table_id".to_owned(),
        };

        assert_eq!(cache.error_code(), ErrorCode::Internal);
        assert_eq!(normalization.error_code(), ErrorCode::Internal);
    }
}
