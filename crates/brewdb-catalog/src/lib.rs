//! BrewDB-owned catalog metadata kernel.

pub(crate) mod backend;
pub(crate) mod cache;
pub mod config;
pub mod errors;
pub mod model;
pub(crate) mod normalize;
pub mod path;
pub mod service;
pub(crate) mod store;

pub use config::{CATALOG_STORE_BACKEND_KEY, CatalogConfig, CatalogStoreBackendKind};
pub use errors::CatalogError;
pub use model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, StorageBinding, TableCatalogEntry,
    TableFormat, TableRef,
};
pub use path::{CatalogPath, DatabasePath, TablePath};
pub use service::{
    AlterTableAction, AlterTableRequest, CatalogResolveContext, CatalogService,
    CreateCatalogRequest, CreateDatabaseRequest, CreateTableRequest, DefaultCatalogService,
    DropTableRequest, TableTarget, UnresolvedTableName,
};

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{CatalogResolveContext, TableTarget, UnresolvedTableName};

    #[test]
    fn crate_root_exports_catalog_service_contracts() {
        let _ = CatalogResolveContext::new(Some("prod"), Some("sales"));
        let _ = UnresolvedTableName::Table("orders".to_owned());
        let _ = TableTarget::ById(Uuid::nil());
    }
}
