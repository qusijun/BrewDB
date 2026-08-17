//! Catalog-facing resolve service.

use std::sync::Arc;
use uuid::Uuid;

use crate::common::config::{global_config_registry, ConfigSet, ConfigView};
use crate::common::defaults::DEFAULT_DATABASE_NAME;

use crate::catalog::backend::CatalogStore;
use crate::catalog::catalogs::{Catalog, CatalogRegistry, ManagedPaimonCatalog};
use crate::catalog::config::CatalogConfig;
use crate::catalog::errors::CatalogError;
use crate::catalog::model::{CatalogEntry, CatalogMode, CatalogRef, StorageKind};
use crate::catalog::path::CatalogPath;
use crate::catalog::requests::CreateDatabaseRequest;

#[derive(Clone)]
pub struct CatalogService {
    store: CatalogStore,
    config: ConfigSet,
    registry: CatalogRegistry,
}

impl CatalogService {
    pub fn new(store: CatalogStore) -> Self {
        let config = global_config_registry()
            .expect("global catalog config registry must be valid")
            .materialize_defaults();
        Self::with_config(store, config)
    }

    pub fn with_config(store: CatalogStore, config: ConfigSet) -> Self {
        Self {
            store,
            config,
            registry: CatalogRegistry::default(),
        }
    }

    pub fn with_config_and_default_managed_paimon_catalog(
        store: CatalogStore,
        config: ConfigSet,
        catalog_config: CatalogConfig,
        catalog_name: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        let service = Self::with_config(store, config);
        let catalog_name = catalog_name.into();
        service.ensure_managed_paimon_catalog(&catalog_config, catalog_name.clone())?;
        service.ensure_default_managed_paimon_database(&catalog_name)?;
        Ok(service)
    }

    pub fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        if self.store.get_catalog(&entry.path)?.is_some() {
            return Err(CatalogError::DuplicateCatalog {
                catalog: entry.path.catalog().to_owned(),
            });
        }

        self.store.create_catalog(entry)?;
        Ok(())
    }

    fn ensure_managed_paimon_catalog(
        &self,
        catalog_config: &CatalogConfig,
        catalog_name: String,
    ) -> Result<(), CatalogError> {
        let path = CatalogPath::new(&catalog_name)?;
        if self.store.get_catalog(&path)?.is_none() {
            let entry = CatalogEntry::new(
                Uuid::new_v4(),
                path,
                CatalogMode::Managed,
                StorageKind::Paimon,
            );
            self.create_catalog(entry.clone())?;
            let catalog = Arc::new(ManagedPaimonCatalog::new(
                entry,
                self.store.clone(),
                catalog_config,
            ));
            self.registry.register(catalog);
            return Ok(());
        }

        let entry = self.resolve_catalog(&path)?;
        if !matches!(
            (entry.mode, entry.storage_kind),
            (CatalogMode::Managed, StorageKind::Paimon)
        ) {
            return Err(CatalogError::CatalogNotRegistered {
                catalog: catalog_name,
            });
        }
        let catalog = Arc::new(ManagedPaimonCatalog::new(
            entry,
            self.store.clone(),
            catalog_config,
        ));
        self.registry.register(catalog);
        Ok(())
    }

    fn ensure_default_managed_paimon_database(
        &self,
        catalog_name: &str,
    ) -> Result<(), CatalogError> {
        let catalog = self.open_catalog(catalog_name)?;
        match catalog.get_database(DEFAULT_DATABASE_NAME) {
            Ok(_) => Ok(()),
            Err(CatalogError::DatabaseNotFound { .. }) => {
                catalog.create_database(CreateDatabaseRequest::new(DEFAULT_DATABASE_NAME))?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub fn resolve_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError> {
        self.store
            .get_catalog(path)?
            .ok_or_else(|| CatalogError::CatalogNotFound {
                catalog: path.catalog().to_owned(),
            })
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

    pub fn open_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>, CatalogError> {
        if let Some(catalog) = self.registry.get(catalog_name) {
            return Ok(catalog);
        }

        let path = CatalogPath::new(catalog_name)?;
        let entry = self.resolve_catalog(&path)?;
        let catalog_config = CatalogConfig::from_config_set(&self.config)?;

        let catalog: Arc<dyn Catalog> = match (entry.mode, entry.storage_kind) {
            (CatalogMode::Managed, StorageKind::Paimon) => Arc::new(ManagedPaimonCatalog::new(
                entry,
                self.store.clone(),
                &catalog_config,
            )),
            _ => {
                return Err(CatalogError::CatalogNotRegistered {
                    catalog: catalog_name.to_owned(),
                });
            }
        };
        self.registry.register(catalog.clone());
        Ok(catalog)
    }

    pub fn list_catalogs(&self) -> Result<Vec<CatalogEntry>, CatalogError> {
        self.store.list_catalogs()
    }

    pub fn list_databases(
        &self,
        catalog_name: &str,
    ) -> Result<Vec<crate::catalog::model::DatabaseCatalogEntry>, CatalogError> {
        let path = CatalogPath::new(catalog_name)?;
        self.store.list_databases(&path)
    }

    pub fn list_tables(
        &self,
        catalog_name: &str,
        database_name: &str,
    ) -> Result<Vec<crate::catalog::model::TableCatalogEntry>, CatalogError> {
        let path = crate::catalog::path::DatabasePath::new(catalog_name, database_name)?;
        self.store.list_tables(&path)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::common::config::{global_config_registry, ConfigPatch, ConfigScope};
    use crate::common::defaults::DEFAULT_DATABASE_NAME;

    use crate::catalog::backend::CatalogStore;
    use crate::catalog::errors::CatalogError;
    use crate::catalog::model::{CatalogEntry, CatalogMode, StorageKind};
    use crate::catalog::path::CatalogPath;
    use crate::catalog::requests::{
        AlterTableOperation, AlterTableRequest, CreateDatabaseRequest, CreateTableRequest,
        RenameTableRequest,
    };
    use crate::catalog::store::memory::MemoryCatalogStoreBackend;

    use super::CatalogService;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&path).expect("test directory must be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn service() -> CatalogService {
        CatalogService::new(CatalogStore::new(Arc::new(
            MemoryCatalogStoreBackend::default(),
        )))
    }

    fn filesystem_paimon_service(warehouse: &Path) -> CatalogService {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();
        CatalogService::with_config(
            CatalogStore::new(Arc::new(MemoryCatalogStoreBackend::default())),
            config,
        )
    }

    #[test]
    fn catalog_service_creates_and_resolves_catalog() {
        let service = service();
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            StorageKind::Paimon,
        );

        service.create_catalog(entry.clone()).unwrap();

        assert_eq!(
            service
                .resolve_catalog(&CatalogPath::new("prod").unwrap())
                .unwrap(),
            entry
        );
    }

    #[test]
    fn catalog_service_opens_managed_paimon_catalog() {
        let service = service();
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            StorageKind::Paimon,
        );
        service.create_catalog(entry).unwrap();

        let catalog = service.open_catalog("prod").unwrap();

        assert_eq!(catalog.entry().path.catalog(), "prod");
    }

    #[test]
    fn catalog_service_reports_missing_catalog() {
        let service = service();

        let error = match service.open_catalog("missing") {
            Ok(_) => panic!("expected missing catalog error"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            CatalogError::CatalogNotFound {
                catalog: "missing".to_owned(),
            }
        );
    }

    #[test]
    fn catalog_service_runs_filesystem_paimon_end_to_end() {
        let warehouse = TestDir::new("brewdb-paimon-e2e");
        let service = filesystem_paimon_service(warehouse.path());
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            StorageKind::Paimon,
        );
        service.create_catalog(entry).unwrap();

        let catalog = service.open_catalog("prod").unwrap();
        let database = catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let table = catalog
            .create_table(
                CreateTableRequest::new(
                    "sales",
                    "orders",
                    TableSchema::new(vec![
                        ColumnField::new("id", DataType::Int32).with_nullable(false),
                        ColumnField::new("name", DataType::String),
                    ]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();

        let fetched = catalog.get_table("sales", "orders").unwrap();
        let renamed = catalog
            .rename_table(RenameTableRequest::new(
                "sales",
                "orders",
                "sales",
                "orders_v2",
            ))
            .unwrap();
        let altered = catalog
            .alter_table(AlterTableRequest::new(
                "sales",
                "orders_v2",
                vec![AlterTableOperation::SetTableOption {
                    key: "bucket".to_owned(),
                    value: "2".to_owned(),
                }],
            ))
            .unwrap();

        assert_eq!(database.path.to_string(), "prod.sales");
        assert_eq!(table.path.to_string(), "prod.sales.orders");
        let warehouse_prefix = warehouse.path().to_string_lossy().to_string();
        assert!(table.table_location.starts_with(&warehouse_prefix));
        assert_eq!(fetched.table_id, table.table_id);
        assert_eq!(renamed.path.to_string(), "prod.sales.orders_v2");
        assert_eq!(altered.table_id, table.table_id);
        assert_eq!(altered.path.to_string(), "prod.sales.orders_v2");
        assert_eq!(altered.table_schema.fields.len(), 2);
        assert_eq!(
            altered.table_options.get("bucket").map(String::as_str),
            Some("2")
        );

        catalog.drop_table("sales", "orders_v2").unwrap();
        catalog.drop_database("sales").unwrap();
    }

    #[test]
    fn catalog_service_bootstraps_default_database() {
        let warehouse = TestDir::new("brewdb-paimon-default-db");
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.path().to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();

        let service = CatalogService::with_config_and_default_managed_paimon_catalog(
            CatalogStore::new(Arc::new(MemoryCatalogStoreBackend::default())),
            config,
            crate::catalog::config::CatalogConfig {
                store_backend: crate::catalog::config::CatalogStoreBackendKind::Memory,
                paimon_warehouse: warehouse.path().to_string_lossy().into_owned(),
            },
            "prod",
        )
        .unwrap();

        let catalog = service.open_catalog("prod").unwrap();
        assert!(catalog.get_database(DEFAULT_DATABASE_NAME).is_ok());
    }
}
