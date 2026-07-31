//! Normalization boundary from catalog-store records into catalog-facing models.

use crate::backend::CatalogStore;
use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone)]
pub struct NormalizedCatalogStore {
    store: CatalogStore,
}

impl NormalizedCatalogStore {
    pub fn new(store: CatalogStore) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &CatalogStore {
        &self.store
    }

    pub fn load_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError> {
        self.store.get_catalog(path)
    }

    pub fn load_catalog_by_ref(
        &self,
        catalog_ref: CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError> {
        self.store.get_catalog_by_ref(catalog_ref)
    }

    pub fn load_database(
        &self,
        path: &DatabasePath,
    ) -> Result<Option<DatabaseEntry>, CatalogError> {
        self.store.get_database(path)
    }

    pub fn load_database_by_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseEntry>, CatalogError> {
        self.store.get_database_by_ref(database_ref)
    }

    pub fn load_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.store.get_table(path)
    }

    pub fn load_table_by_ref(
        &self,
        table_ref: TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.store.get_table_by_ref(table_ref)
    }
}
