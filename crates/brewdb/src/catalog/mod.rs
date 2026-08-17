//! BrewDB-owned catalog metadata kernel.

pub mod backend;
pub mod catalogs;
pub mod config;
pub mod errors;
pub mod model;
mod paimon_schema;
pub mod path;
pub mod requests;
pub mod service;
mod storage_format_schema;
pub mod store;

pub use backend::{CatalogStore, CatalogStoreBackend};
pub use catalogs::{Catalog, CatalogRegistry, ManagedPaimonCatalog};
pub use config::{
    CatalogConfig, CatalogStoreBackendKind, CATALOG_PAIMON_WAREHOUSE_KEY, CATALOG_STORE_BACKEND_KEY,
};
pub use errors::CatalogError;
pub use model::{
    CatalogEntry, CatalogMode, CatalogRef, DatabaseCatalogEntry, DatabaseRef, StorageKind,
    TableCatalogEntry, TableRef, TableStatsHandle, TableSummary,
};
pub use paimon_schema::PaimonSchemaAdapter;
pub use path::{CatalogPath, DatabasePath, TablePath};
pub use requests::{
    AlterTableOperation, AlterTableRequest, ColumnDefinition, CreateDatabaseRequest,
    CreateTableRequest, RenameTableRequest, TableDefinition,
};
pub use service::CatalogService;
pub use storage_format_schema::StorageFormatSchemaAdapter;
pub use store::open_catalog_store;
