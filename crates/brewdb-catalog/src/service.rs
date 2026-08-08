//! Catalog-facing resolve service.

use std::sync::Arc;

use brewdb_common::config::{ConfigSet, ConfigView, global_config_registry};

use crate::backend::CatalogStore;
use crate::catalogs::{Catalog, CatalogRegistry, ManagedPaimonCatalog};
use crate::config::CatalogConfig;
use crate::errors::CatalogError;
use crate::model::{CatalogEntry, CatalogMode, CatalogRef, LakeFormatKind};
use crate::path::CatalogPath;

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

    pub fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        if self.store.get_catalog(&entry.path)?.is_some() {
            return Err(CatalogError::DuplicateCatalog {
                catalog: entry.path.catalog().to_owned(),
            });
        }

        self.store.create_catalog(entry)?;
        Ok(())
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

        let catalog: Arc<dyn Catalog> =
            match (entry.mode, entry.lake_format_kind) {
                (CatalogMode::Managed, LakeFormatKind::Paimon) => Arc::new(
                    ManagedPaimonCatalog::new(entry, self.store.clone(), &catalog_config),
                ),
                _ => {
                    return Err(CatalogError::CatalogNotRegistered {
                        catalog: catalog_name.to_owned(),
                    });
                }
            };
        self.registry.register(catalog.clone());
        Ok(catalog)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use brewdb_common::config::{ConfigPatch, ConfigScope, global_config_registry};

    use crate::backend::CatalogStore;
    use crate::errors::CatalogError;
    use crate::model::{CatalogEntry, CatalogMode, LakeFormatKind};
    use crate::path::CatalogPath;
    use crate::requests::{
        AlterTableOperation, AlterTableRequest, CreateDatabaseRequest, CreateTableRequest,
        RenameTableRequest,
    };
    use crate::store::memory::MemoryCatalogStoreBackend;

    use super::CatalogService;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};

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
            LakeFormatKind::Paimon,
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
            LakeFormatKind::Paimon,
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
            LakeFormatKind::Paimon,
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
                        SchemaField::new("id", DataType::Int32).with_nullable(false),
                        SchemaField::new("name", DataType::String),
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
}
