//! In-memory catalog store backend for tests and bootstrap wiring.

use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::backend::CatalogStoreBackend;
use crate::errors::CatalogError;
use crate::model::{CatalogEntry, DatabaseCatalogEntry, TableCatalogEntry};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Default)]
pub struct MemoryCatalogStoreBackend {
    catalogs: RwLock<BTreeMap<String, CatalogEntry>>,
    catalogs_by_id: RwLock<BTreeMap<uuid::Uuid, CatalogEntry>>,
    databases: RwLock<BTreeMap<String, DatabaseCatalogEntry>>,
    databases_by_id: RwLock<BTreeMap<uuid::Uuid, DatabaseCatalogEntry>>,
    tables: RwLock<BTreeMap<String, TableCatalogEntry>>,
    tables_by_id: RwLock<BTreeMap<uuid::Uuid, TableCatalogEntry>>,
}

impl CatalogStoreBackend for MemoryCatalogStoreBackend {
    fn get_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError> {
        Ok(self
            .catalogs
            .read()
            .expect("catalog lock poisoned")
            .get(path.catalog())
            .cloned())
    }

    fn get_catalog_by_ref(
        &self,
        catalog_ref: crate::model::CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError> {
        Ok(self
            .catalogs_by_id
            .read()
            .expect("catalog-by-id lock poisoned")
            .get(&catalog_ref.id())
            .cloned())
    }

    fn get_database(
        &self,
        path: &DatabasePath,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        Ok(self
            .databases
            .read()
            .expect("database lock poisoned")
            .get(&path.to_string())
            .cloned())
    }

    fn get_database_by_ref(
        &self,
        database_ref: crate::model::DatabaseRef,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        Ok(self
            .databases_by_id
            .read()
            .expect("database-by-id lock poisoned")
            .get(&database_ref.id())
            .cloned())
    }

    fn get_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
        Ok(self
            .tables
            .read()
            .expect("table lock poisoned")
            .get(&path.to_string())
            .cloned())
    }

    fn get_table_by_ref(
        &self,
        table_ref: crate::model::TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError> {
        Ok(self
            .tables_by_id
            .read()
            .expect("table-by-id lock poisoned")
            .get(&table_ref.id())
            .cloned())
    }

    fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.catalogs_by_id
            .write()
            .expect("catalog-by-id lock poisoned")
            .insert(entry.catalog_id, entry.clone());
        self.catalogs
            .write()
            .expect("catalog lock poisoned")
            .insert(entry.path.catalog().to_owned(), entry);
        Ok(())
    }

    fn create_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        self.databases_by_id
            .write()
            .expect("database-by-id lock poisoned")
            .insert(entry.database_id, entry.clone());
        self.databases
            .write()
            .expect("database lock poisoned")
            .insert(entry.path.to_string(), entry);
        Ok(())
    }

    fn create_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.tables_by_id
            .write()
            .expect("table-by-id lock poisoned")
            .insert(entry.table_id, entry.clone());
        self.tables
            .write()
            .expect("table lock poisoned")
            .insert(entry.path.to_string(), entry);
        Ok(())
    }

    fn update_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.create_catalog(entry)
    }

    fn update_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        self.create_database(entry)
    }

    fn update_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.create_table(entry)
    }

    fn delete_catalog(&self, path: &CatalogPath) -> Result<(), CatalogError> {
        if let Some(entry) = self
            .catalogs
            .write()
            .expect("catalog lock poisoned")
            .remove(path.catalog())
        {
            self.catalogs_by_id
                .write()
                .expect("catalog-by-id lock poisoned")
                .remove(&entry.catalog_id);
        }
        Ok(())
    }

    fn delete_database(&self, path: &DatabasePath) -> Result<(), CatalogError> {
        if let Some(entry) = self
            .databases
            .write()
            .expect("database lock poisoned")
            .remove(&path.to_string())
        {
            self.databases_by_id
                .write()
                .expect("database-by-id lock poisoned")
                .remove(&entry.database_id);
        }
        Ok(())
    }

    fn delete_table(&self, path: &TablePath) -> Result<(), CatalogError> {
        if let Some(entry) = self
            .tables
            .write()
            .expect("table lock poisoned")
            .remove(&path.to_string())
        {
            self.tables_by_id
                .write()
                .expect("table-by-id lock poisoned")
                .remove(&entry.table_id);
        }
        Ok(())
    }

    fn list_databases(
        &self,
        catalog_path: &CatalogPath,
    ) -> Result<Vec<DatabaseCatalogEntry>, CatalogError> {
        Ok(self
            .databases
            .read()
            .expect("database lock poisoned")
            .values()
            .filter(|entry| entry.path.catalog() == catalog_path.catalog())
            .cloned()
            .collect())
    }

    fn list_tables(
        &self,
        database_path: &DatabasePath,
    ) -> Result<Vec<TableCatalogEntry>, CatalogError> {
        Ok(self
            .tables
            .read()
            .expect("table lock poisoned")
            .values()
            .filter(|entry| {
                entry.path.catalog() == database_path.catalog()
                    && entry.path.database() == database_path.database()
            })
            .cloned()
            .collect())
    }
}
