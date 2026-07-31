//! Catalog-facing resolve service.

use std::sync::Arc;

use crate::backend::CatalogStore;
use crate::cache::{CatalogCacheManager, new_noop_cache_manager};
use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, StorageBinding, TableCatalogEntry,
    TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone)]
pub struct CatalogService {
    store: CatalogStore,
    cache_manager: Arc<dyn CatalogCacheManager>,
}

impl CatalogService {
    pub fn new(store: CatalogStore) -> Self {
        Self {
            store,
            cache_manager: Arc::new(new_noop_cache_manager()),
        }
    }

    pub fn with_cache_manager(
        store: CatalogStore,
        cache_manager: Arc<dyn CatalogCacheManager>,
    ) -> Self {
        Self {
            store,
            cache_manager,
        }
    }

    pub fn store(&self) -> &CatalogStore {
        &self.store
    }

    pub fn cache_manager(&self) -> &Arc<dyn CatalogCacheManager> {
        &self.cache_manager
    }

    pub fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        if self.store.get_catalog(&entry.path)?.is_some() {
            return Err(CatalogError::DuplicateCatalog {
                catalog: entry.path.catalog().to_owned(),
            });
        }
        self.store.put_catalog(entry)
    }

    pub fn create_database(&self, entry: DatabaseEntry) -> Result<(), CatalogError> {
        self.require_catalog(&entry.path.catalog_path())?;
        if self.store.get_database(&entry.path)?.is_some() {
            return Err(CatalogError::DuplicateDatabase {
                catalog: entry.path.catalog().to_owned(),
                database: entry.path.database().to_owned(),
            });
        }
        self.store.put_database(entry)
    }

    pub fn create_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.require_database(&entry.path.database_path())?;
        if self.store.get_table(&entry.path)?.is_some() {
            return Err(CatalogError::DuplicateTable {
                catalog: entry.path.catalog().to_owned(),
                database: entry.path.database().to_owned(),
                table: entry.path.table().to_owned(),
            });
        }
        self.store.put_table(entry)
    }

    pub fn resolve_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_catalog(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }
        self.cache_manager.record_miss();
        self.require_catalog(path)
    }

    pub fn resolve_catalog_ref(
        &self,
        catalog_ref: CatalogRef,
    ) -> Result<CatalogEntry, CatalogError> {
        self.store.get_catalog_by_ref(catalog_ref)?.ok_or_else(|| {
            CatalogError::CatalogRefNotFound {
                catalog_id: catalog_ref.id().to_string(),
            }
        })
    }

    pub fn resolve_database(&self, path: &DatabasePath) -> Result<DatabaseEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_database(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }
        self.cache_manager.record_miss();
        self.require_database(path)
    }

    pub fn resolve_database_ref(
        &self,
        database_ref: DatabaseRef,
    ) -> Result<DatabaseEntry, CatalogError> {
        self.store
            .get_database_by_ref(database_ref)?
            .ok_or_else(|| CatalogError::DatabaseRefNotFound {
                database_id: database_ref.id().to_string(),
            })
    }

    pub fn resolve_table(&self, path: &TablePath) -> Result<TableCatalogEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_table(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }
        self.cache_manager.record_miss();
        self.store
            .get_table(path)?
            .ok_or_else(|| CatalogError::TableNotFound {
                catalog: path.catalog().to_owned(),
                database: path.database().to_owned(),
                table: path.table().to_owned(),
            })
    }

    pub fn resolve_table_ref(
        &self,
        table_ref: TableRef,
    ) -> Result<TableCatalogEntry, CatalogError> {
        self.store
            .get_table_by_ref(table_ref)?
            .ok_or_else(|| CatalogError::TableRefNotFound {
                table_id: table_ref.id().to_string(),
            })
    }

    pub fn resolve_storage_binding(
        &self,
        path: &TablePath,
    ) -> Result<StorageBinding, CatalogError> {
        Ok(self.resolve_table(path)?.storage)
    }

    fn require_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError> {
        self.store
            .get_catalog(path)?
            .ok_or_else(|| CatalogError::CatalogNotFound {
                catalog: path.catalog().to_owned(),
            })
    }

    fn require_database(&self, path: &DatabasePath) -> Result<DatabaseEntry, CatalogError> {
        self.require_catalog(&path.catalog_path())?;
        self.store
            .get_database(path)?
            .ok_or_else(|| CatalogError::DatabaseNotFound {
                catalog: path.catalog().to_owned(),
                database: path.database().to_owned(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::backend::CatalogStore;
    use crate::cache::new_noop_cache_manager;
    use crate::model::{
        CatalogEntry, DatabaseEntry, StorageBinding, TableCatalogEntry, TableFormat,
    };
    use crate::path::{CatalogPath, DatabasePath, TablePath};
    use crate::store::memory::MemoryCatalogStoreBackend;

    use super::CatalogService;

    #[test]
    fn catalog_service_resolves_catalog_database_and_table() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = CatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CatalogEntry::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(DatabaseEntry::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table_path = TablePath::new("prod", "sales", "orders").unwrap();
        service
            .create_table(TableCatalogEntry::new(
                Uuid::new_v4(),
                table_path.clone(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let table = service.resolve_table(&table_path).unwrap();

        assert_eq!(table.path.to_string(), "prod.sales.orders");
        assert_eq!(table.storage.location, "s3://warehouse/orders");
    }

    #[test]
    fn catalog_service_resolves_table_ref_and_storage_binding() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = CatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CatalogEntry::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(DatabaseEntry::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table = TableCatalogEntry::new(
            Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
        );
        let table_ref = table.table_ref();
        service.create_table(table.clone()).unwrap();

        let resolved = service.resolve_table_ref(table_ref).unwrap();
        let storage = service.resolve_storage_binding(&table.path).unwrap();

        assert_eq!(resolved.table_id, table.table_id);
        assert_eq!(storage.location, "s3://warehouse/orders");
        assert_eq!(storage.format, TableFormat::Paimon);
    }

    #[test]
    fn table_creation_requires_parent_database() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = CatalogService::new(CatalogStore::new(backend));
        let table = TableCatalogEntry::new(
            Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
        );

        let error = service.create_table(table).unwrap_err();

        assert_eq!(error.to_string(), "catalog not found: `prod`");
    }

    #[test]
    fn catalog_service_exposes_cache_manager_boundary() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let cache_manager: Arc<dyn crate::cache::CatalogCacheManager> =
            Arc::new(new_noop_cache_manager());
        let service =
            CatalogService::with_cache_manager(CatalogStore::new(backend), cache_manager.clone());

        assert!(Arc::ptr_eq(service.cache_manager(), &cache_manager));
    }
}
