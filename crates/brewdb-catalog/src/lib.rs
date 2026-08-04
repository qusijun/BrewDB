//! BrewDB-owned catalog metadata kernel.

pub mod backend;
pub mod catalogs;
pub mod config;
pub mod errors;
pub mod model;
pub mod path;
pub mod requests;
pub mod service;
pub mod store;

pub use backend::{CatalogStore, CatalogStoreBackend};
pub use catalogs::{Catalog, CatalogRegistry, ManagedPaimonCatalog};
pub use config::{
    CATALOG_PAIMON_WAREHOUSE_KEY, CATALOG_STORE_BACKEND_KEY, CatalogConfig, CatalogStoreBackendKind,
};
pub use errors::CatalogError;
pub use model::{
    CatalogEntry, CatalogMode, CatalogRef, DatabaseCatalogEntry, DatabaseRef, LakeFormatKind,
    TableCatalogEntry, TableRef,
};
pub use path::{CatalogPath, DatabasePath, TablePath};
pub use requests::{
    AlterTableOperation, AlterTableRequest, ColumnDefinition, CreateDatabaseRequest,
    CreateTableRequest, RenameTableRequest, TableDefinition,
};
pub use service::CatalogService;
pub use store::open_catalog_store;
