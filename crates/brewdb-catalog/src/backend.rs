//! Catalog store and backend contracts.

use std::sync::Arc;

use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

pub trait CatalogStoreBackend: Send + Sync {
    fn get_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError>;

    fn get_catalog_by_ref(
        &self,
        catalog_ref: CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError>;

    fn get_database(&self, path: &DatabasePath) -> Result<Option<DatabaseEntry>, CatalogError>;

    fn get_database_by_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseEntry>, CatalogError>;

    fn get_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError>;

    fn get_table_by_ref(
        &self,
        table_ref: TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError>;

    fn put_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError>;

    fn put_database(&self, entry: DatabaseEntry) -> Result<(), CatalogError>;

    fn put_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError>;
}

#[derive(Clone)]
pub struct CatalogStore {
    backend: Arc<dyn CatalogStoreBackend>,
}

impl CatalogStore {
    pub fn new(backend: Arc<dyn CatalogStoreBackend>) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &Arc<dyn CatalogStoreBackend> {
        &self.backend
    }

    pub fn get_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError> {
        self.backend.get_catalog(path)
    }

    pub fn get_catalog_by_ref(
        &self,
        catalog_ref: CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError> {
        self.backend.get_catalog_by_ref(catalog_ref)
    }

    pub fn get_database(&self, path: &DatabasePath) -> Result<Option<DatabaseEntry>, CatalogError> {
        self.backend.get_database(path)
    }

    pub fn get_database_by_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseEntry>, CatalogError> {
        self.backend.get_database_by_ref(database_ref)
    }

    pub fn get_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.backend.get_table(path)
    }

    pub fn get_table_by_ref(
        &self,
        table_ref: TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.backend.get_table_by_ref(table_ref)
    }

    pub fn put_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.backend.put_catalog(entry)
    }

    pub fn put_database(&self, entry: DatabaseEntry) -> Result<(), CatalogError> {
        self.backend.put_database(entry)
    }

    pub fn put_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.backend.put_table(entry)
    }
}
