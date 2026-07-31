//! BrewDB-owned catalog metadata kernel.

pub mod backend;
pub mod cache;
pub mod config;
pub mod errors;
pub mod model;
pub mod normalize;
pub mod path;
pub mod service;
pub mod store;

pub use backend::{CatalogStore, CatalogStoreBackend};
pub use config::{CATALOG_STORE_BACKEND_KEY, CatalogConfig, CatalogStoreBackendKind};
pub use errors::CatalogError;
pub use model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, StorageBinding, TableCatalogEntry,
    TableFormat, TableRef,
};
pub use path::{CatalogPath, DatabasePath, TablePath};
pub use service::CatalogService;
pub use store::open_catalog_store;
