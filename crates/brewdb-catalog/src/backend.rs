//! Catalog store and backend contracts.

use std::sync::Arc;

use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseCatalogEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

pub trait CatalogStoreBackend: Send + Sync {
    fn get_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError>;

    fn get_catalog_by_ref(
        &self,
        catalog_ref: CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError>;

    fn get_database(
        &self,
        path: &DatabasePath,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError>;

    fn get_database_by_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError>;

    fn get_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError>;

    fn get_table_by_ref(
        &self,
        table_ref: TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError>;

    fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError>;

    fn create_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError>;

    fn create_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError>;

    fn update_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError>;

    fn update_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError>;

    fn update_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError>;

    fn delete_catalog(&self, path: &CatalogPath) -> Result<(), CatalogError>;

    fn delete_database(&self, path: &DatabasePath) -> Result<(), CatalogError>;

    fn delete_table(&self, path: &TablePath) -> Result<(), CatalogError>;

    fn list_databases(
        &self,
        catalog_path: &CatalogPath,
    ) -> Result<Vec<DatabaseCatalogEntry>, CatalogError>;

    fn list_tables(
        &self,
        database_path: &DatabasePath,
    ) -> Result<Vec<TableCatalogEntry>, CatalogError>;

    fn database_exists(&self, path: &DatabasePath) -> Result<bool, CatalogError> {
        Ok(self.get_database(path)?.is_some())
    }

    fn table_exists(&self, path: &TablePath) -> Result<bool, CatalogError> {
        Ok(self.get_table(path)?.is_some())
    }
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

    pub fn get_database(
        &self,
        path: &DatabasePath,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        self.backend.get_database(path)
    }

    pub fn get_database_by_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
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

    pub fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.backend.create_catalog(entry)
    }

    pub fn create_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        self.backend.create_database(entry)
    }

    pub fn create_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.backend.create_table(entry)
    }

    pub fn update_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.backend.update_catalog(entry)
    }

    pub fn update_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        self.backend.update_database(entry)
    }

    pub fn update_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.backend.update_table(entry)
    }

    pub fn delete_catalog(&self, path: &CatalogPath) -> Result<(), CatalogError> {
        self.backend.delete_catalog(path)
    }

    pub fn delete_database(&self, path: &DatabasePath) -> Result<(), CatalogError> {
        self.backend.delete_database(path)
    }

    pub fn delete_table(&self, path: &TablePath) -> Result<(), CatalogError> {
        self.backend.delete_table(path)
    }

    pub fn list_databases(
        &self,
        catalog_path: &CatalogPath,
    ) -> Result<Vec<DatabaseCatalogEntry>, CatalogError> {
        self.backend.list_databases(catalog_path)
    }

    pub fn list_tables(
        &self,
        database_path: &DatabasePath,
    ) -> Result<Vec<TableCatalogEntry>, CatalogError> {
        self.backend.list_tables(database_path)
    }

    pub fn database_exists(&self, path: &DatabasePath) -> Result<bool, CatalogError> {
        self.backend.database_exists(path)
    }

    pub fn table_exists(&self, path: &TablePath) -> Result<bool, CatalogError> {
        self.backend.table_exists(path)
    }
}
