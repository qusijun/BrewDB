//! Public catalog service contract and default implementation.

use std::sync::Arc;

use uuid::Uuid;

use crate::backend::CatalogStore;
use crate::cache::{CatalogCacheManager, new_noop_cache_manager};
use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, StorageBinding, TableCatalogEntry,
    TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnresolvedTableName {
    Table(String),
    DatabaseTable { database: String, table: String },
    CatalogDatabaseTable {
        catalog: String,
        database: String,
        table: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogResolveContext {
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
}

impl CatalogResolveContext {
    pub fn new(
        default_catalog: Option<impl Into<String>>,
        default_database: Option<impl Into<String>>,
    ) -> Self {
        Self {
            default_catalog: default_catalog.map(Into::into),
            default_database: default_database.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogRequest {
    pub catalog_id: Uuid,
    pub path: CatalogPath,
}

impl CreateCatalogRequest {
    pub fn new(catalog_id: Uuid, path: CatalogPath) -> Self {
        Self { catalog_id, path }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseRequest {
    pub database_id: Uuid,
    pub path: DatabasePath,
}

impl CreateDatabaseRequest {
    pub fn new(database_id: Uuid, path: DatabasePath) -> Self {
        Self { database_id, path }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableRequest {
    pub table_id: Uuid,
    pub path: TablePath,
    pub storage: StorageBinding,
}

impl CreateTableRequest {
    pub fn new(table_id: Uuid, path: TablePath, storage: StorageBinding) -> Self {
        Self {
            table_id,
            path,
            storage,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableTarget {
    ById(Uuid),
    ByPath(TablePath),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableRequest {
    pub target: TableTarget,
    pub if_exists: bool,
}

impl DropTableRequest {
    pub fn new(target: TableTarget, if_exists: bool) -> Self {
        Self { target, if_exists }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableAction {
    Rename { new_path: TablePath },
    ReplaceSchema,
    SetProperties,
    RemoveProperties,
    UpdateStorageBinding { storage: StorageBinding },
    SetState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableRequest {
    pub target: TableTarget,
    pub action: AlterTableAction,
}

impl AlterTableRequest {
    pub fn new(target: TableTarget, action: AlterTableAction) -> Self {
        Self { target, action }
    }
}

pub trait CatalogService: Send + Sync {
    fn get_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError>;

    fn get_database(&self, path: &DatabasePath) -> Result<DatabaseEntry, CatalogError>;

    fn get_table(&self, path: &TablePath) -> Result<TableCatalogEntry, CatalogError>;

    fn get_catalog_by_id(&self, catalog_id: Uuid) -> Result<CatalogEntry, CatalogError>;

    fn get_database_by_id(&self, database_id: Uuid) -> Result<DatabaseEntry, CatalogError>;

    fn get_table_by_id(&self, table_id: Uuid) -> Result<TableCatalogEntry, CatalogError>;

    fn resolve_table(
        &self,
        name: UnresolvedTableName,
        ctx: &CatalogResolveContext,
    ) -> Result<TableCatalogEntry, CatalogError>;

    fn create_catalog(&self, req: CreateCatalogRequest) -> Result<CatalogEntry, CatalogError>;

    fn create_database(&self, req: CreateDatabaseRequest) -> Result<DatabaseEntry, CatalogError>;

    fn create_table(&self, req: CreateTableRequest) -> Result<TableCatalogEntry, CatalogError>;

    fn alter_table(&self, req: AlterTableRequest) -> Result<TableCatalogEntry, CatalogError>;

    fn drop_table(&self, req: DropTableRequest) -> Result<(), CatalogError>;
}

#[derive(Clone)]
pub struct DefaultCatalogService {
    store: CatalogStore,
    cache_manager: Arc<dyn CatalogCacheManager>,
}

impl DefaultCatalogService {
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

    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &CatalogStore {
        &self.store
    }

    #[allow(dead_code)]
    pub(crate) fn cache_manager(&self) -> &Arc<dyn CatalogCacheManager> {
        &self.cache_manager
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

    fn resolve_table_path(
        &self,
        name: UnresolvedTableName,
        ctx: &CatalogResolveContext,
    ) -> Result<TablePath, CatalogError> {
        match name {
            UnresolvedTableName::Table(table) => {
                let catalog = ctx.default_catalog.clone().ok_or_else(|| {
                    CatalogError::InvalidTableNameResolution {
                        name: table.clone(),
                        reason: "default catalog is not set".to_owned(),
                    }
                })?;
                let database = ctx.default_database.clone().ok_or_else(|| {
                    CatalogError::InvalidTableNameResolution {
                        name: table.clone(),
                        reason: "default database is not set".to_owned(),
                    }
                })?;
                TablePath::new(catalog, database, table)
            }
            UnresolvedTableName::DatabaseTable { database, table } => {
                let name = format!("{database}.{table}");
                let catalog = ctx.default_catalog.clone().ok_or_else(|| {
                    CatalogError::InvalidTableNameResolution {
                        name: name.clone(),
                        reason: "default catalog is not set".to_owned(),
                    }
                })?;
                TablePath::new(catalog, database, table)
            }
            UnresolvedTableName::CatalogDatabaseTable {
                catalog,
                database,
                table,
            } => TablePath::new(catalog, database, table),
        }
    }

    fn lookup_table_target(&self, target: &TableTarget) -> Result<Option<TableCatalogEntry>, CatalogError> {
        match target {
            TableTarget::ById(table_id) => self
                .store
                .get_table_by_ref(TableRef::new(*table_id)),
            TableTarget::ByPath(path) => self.store.get_table(path),
        }
    }

    fn cache_catalog(&self, entry: &CatalogEntry) {
        self.cache_manager.cache().put_catalog(entry.clone());
    }

    fn cache_database(&self, entry: &DatabaseEntry) {
        self.cache_manager.cache().put_database(entry.clone());
    }

    fn cache_table(&self, entry: &TableCatalogEntry) {
        self.cache_manager.cache().put_table(entry.clone());
    }
}

impl CatalogService for DefaultCatalogService {
    fn get_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_catalog(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self.require_catalog(path)?;
        self.cache_catalog(&entry);
        Ok(entry)
    }

    fn get_database(&self, path: &DatabasePath) -> Result<DatabaseEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_database(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self.require_database(path)?;
        self.cache_database(&entry);
        Ok(entry)
    }

    fn get_table(&self, path: &TablePath) -> Result<TableCatalogEntry, CatalogError> {
        if let Some(entry) = self.cache_manager.cache().get_table(path) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self
            .store
            .get_table(path)?
            .ok_or_else(|| CatalogError::TableNotFound {
                catalog: path.catalog().to_owned(),
                database: path.database().to_owned(),
                table: path.table().to_owned(),
            })?;
        self.cache_table(&entry);
        Ok(entry)
    }

    fn get_catalog_by_id(&self, catalog_id: Uuid) -> Result<CatalogEntry, CatalogError> {
        let catalog_ref = CatalogRef::new(catalog_id);
        if let Some(entry) = self.cache_manager.cache().get_catalog_by_ref(catalog_ref) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self
            .store
            .get_catalog_by_ref(catalog_ref)?
            .ok_or_else(|| CatalogError::CatalogRefNotFound {
                catalog_id: catalog_id.to_string(),
            })?;
        self.cache_catalog(&entry);
        Ok(entry)
    }

    fn get_database_by_id(&self, database_id: Uuid) -> Result<DatabaseEntry, CatalogError> {
        let database_ref = DatabaseRef::new(database_id);
        if let Some(entry) = self.cache_manager.cache().get_database_by_ref(database_ref) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self
            .store
            .get_database_by_ref(database_ref)?
            .ok_or_else(|| CatalogError::DatabaseRefNotFound {
                database_id: database_id.to_string(),
            })?;
        self.cache_database(&entry);
        Ok(entry)
    }

    fn get_table_by_id(&self, table_id: Uuid) -> Result<TableCatalogEntry, CatalogError> {
        let table_ref = TableRef::new(table_id);
        if let Some(entry) = self.cache_manager.cache().get_table_by_ref(table_ref) {
            self.cache_manager.record_hit();
            return Ok(entry);
        }

        self.cache_manager.record_miss();
        let entry = self
            .store
            .get_table_by_ref(table_ref)?
            .ok_or_else(|| CatalogError::TableRefNotFound {
                table_id: table_id.to_string(),
            })?;
        self.cache_table(&entry);
        Ok(entry)
    }

    fn resolve_table(
        &self,
        name: UnresolvedTableName,
        ctx: &CatalogResolveContext,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let path = self.resolve_table_path(name, ctx)?;
        self.get_table(&path)
    }

    fn create_catalog(&self, req: CreateCatalogRequest) -> Result<CatalogEntry, CatalogError> {
        if self.store.get_catalog(&req.path)?.is_some() {
            return Err(CatalogError::DuplicateCatalog {
                catalog: req.path.catalog().to_owned(),
            });
        }

        let entry = CatalogEntry::new(req.catalog_id, req.path);
        self.store.put_catalog(entry.clone())?;
        self.cache_catalog(&entry);
        Ok(entry)
    }

    fn create_database(&self, req: CreateDatabaseRequest) -> Result<DatabaseEntry, CatalogError> {
        self.require_catalog(&req.path.catalog_path())?;
        if self.store.get_database(&req.path)?.is_some() {
            return Err(CatalogError::DuplicateDatabase {
                catalog: req.path.catalog().to_owned(),
                database: req.path.database().to_owned(),
            });
        }

        let entry = DatabaseEntry::new(req.database_id, req.path);
        self.store.put_database(entry.clone())?;
        self.cache_database(&entry);
        Ok(entry)
    }

    fn create_table(&self, req: CreateTableRequest) -> Result<TableCatalogEntry, CatalogError> {
        self.require_database(&req.path.database_path())?;
        if self.store.get_table(&req.path)?.is_some() {
            return Err(CatalogError::DuplicateTable {
                catalog: req.path.catalog().to_owned(),
                database: req.path.database().to_owned(),
                table: req.path.table().to_owned(),
            });
        }

        let entry = TableCatalogEntry::new(req.table_id, req.path, req.storage);
        self.store.put_table(entry.clone())?;
        self.cache_table(&entry);
        Ok(entry)
    }

    fn alter_table(&self, _req: AlterTableRequest) -> Result<TableCatalogEntry, CatalogError> {
        Err(CatalogError::UnsupportedCatalogOperation {
            operation: "alter_table",
        })
    }

    fn drop_table(&self, req: DropTableRequest) -> Result<(), CatalogError> {
        let path = match req.target {
            TableTarget::ByPath(path) => path,
            TableTarget::ById(table_id) => {
                let entry = self
                    .lookup_table_target(&TableTarget::ById(table_id))?
                    .ok_or_else(|| CatalogError::TableRefNotFound {
                        table_id: table_id.to_string(),
                    })?;
                entry.path
            }
        };

        let deleted = self.store.delete_table(&path)?;
        if deleted.is_none() {
            if req.if_exists {
                return Ok(());
            }
            return Err(CatalogError::TableNotFound {
                catalog: path.catalog().to_owned(),
                database: path.database().to_owned(),
                table: path.table().to_owned(),
            });
        }

        self.cache_manager.invalidate_table(&path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::backend::CatalogStore;
    use crate::cache::new_noop_cache_manager;
    use crate::model::{StorageBinding, TableFormat};
    use crate::path::{CatalogPath, DatabasePath, TablePath};
    use crate::store::memory::MemoryCatalogStoreBackend;

    use super::{
        CatalogResolveContext, CatalogService, CreateCatalogRequest, CreateDatabaseRequest,
        CreateTableRequest, DefaultCatalogService, DropTableRequest, TableTarget,
        UnresolvedTableName,
    };

    #[test]
    fn catalog_service_resolves_catalog_database_and_table() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table_path = TablePath::new("prod", "sales", "orders").unwrap();
        service
            .create_table(CreateTableRequest::new(
                Uuid::new_v4(),
                table_path.clone(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let table = service.get_table(&table_path).unwrap();

        assert_eq!(table.path.to_string(), "prod.sales.orders");
        assert_eq!(table.storage.location, "s3://warehouse/orders");
    }

    #[test]
    fn catalog_service_resolves_table_ref_and_storage_binding() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table_id = Uuid::new_v4();
        service
            .create_table(CreateTableRequest::new(
                table_id,
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let resolved = service.get_table_by_id(table_id).unwrap();

        assert_eq!(resolved.table_id, table_id);
        assert_eq!(resolved.storage.location, "s3://warehouse/orders");
        assert_eq!(resolved.storage.format, TableFormat::Paimon);
    }

    #[test]
    fn table_creation_requires_parent_database() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        let error = service
            .create_table(CreateTableRequest::new(
                Uuid::new_v4(),
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap_err();

        assert_eq!(error.to_string(), "catalog not found: `prod`");
    }

    #[test]
    fn catalog_service_exposes_cache_manager_boundary() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let cache_manager: Arc<dyn crate::cache::CatalogCacheManager> =
            Arc::new(new_noop_cache_manager());
        let service = DefaultCatalogService::with_cache_manager(
            CatalogStore::new(backend),
            cache_manager.clone(),
        );

        assert!(Arc::ptr_eq(service.cache_manager(), &cache_manager));
        let _ = service.store();
    }

    #[test]
    fn resolve_table_binds_default_catalog_and_database() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        service
            .create_table(CreateTableRequest::new(
                Uuid::new_v4(),
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let resolved = service
            .resolve_table(
                UnresolvedTableName::Table("orders".to_owned()),
                &CatalogResolveContext::new(Some("prod"), Some("sales")),
            )
            .unwrap();

        assert_eq!(resolved.path.to_string(), "prod.sales.orders");
    }

    #[test]
    fn get_table_by_id_returns_table_entry() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table_id = Uuid::new_v4();
        service
            .create_table(CreateTableRequest::new(
                table_id,
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let table = service.get_table_by_id(table_id).unwrap();

        assert_eq!(table.table_id, table_id);
    }

    #[test]
    fn drop_table_request_supports_if_exists() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        let result = service.drop_table(DropTableRequest::new(
            TableTarget::ByPath(TablePath::new("prod", "sales", "missing").unwrap()),
            true,
        ));

        assert!(result.is_ok());
    }

    #[test]
    fn catalog_service_trait_object_uses_default_implementation() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service: Arc<dyn CatalogService> =
            Arc::new(DefaultCatalogService::new(CatalogStore::new(backend)));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();

        let catalog = service.get_catalog(&CatalogPath::new("prod").unwrap()).unwrap();

        assert_eq!(catalog.path.to_string(), "prod");
    }
}
