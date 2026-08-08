//! Catalog implementations and runtime registry.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, RwLock};

use paimon::CatalogFactory as PaimonCatalogFactory;
use paimon::catalog::{Catalog as PaimonCatalog, Database as PaimonDatabase, Identifier};
use paimon::spec::SchemaChange;
use paimon::table::Table as PaimonTable;

use crate::backend::CatalogStore;
use crate::config::CatalogConfig;
use crate::errors::CatalogError;
use crate::model::{CatalogEntry, DatabaseCatalogEntry, LakeFormatKind, TableCatalogEntry};
use crate::paimon_schema::PaimonSchemaAdapter;
use crate::path::{DatabasePath, TablePath};
use crate::requests::{
    AlterTableOperation, AlterTableRequest, CreateDatabaseRequest, CreateTableRequest,
    RenameTableRequest,
};
use crate::storage_format_schema::StorageFormatSchemaAdapter;

pub trait Catalog: Send + Sync {
    fn entry(&self) -> &CatalogEntry;

    fn get_database(&self, database_name: &str) -> Result<DatabaseCatalogEntry, CatalogError>;

    fn get_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<TableCatalogEntry, CatalogError>;

    fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<DatabaseCatalogEntry, CatalogError>;

    fn create_table(&self, request: CreateTableRequest) -> Result<TableCatalogEntry, CatalogError>;

    fn drop_database(&self, database_name: &str) -> Result<(), CatalogError>;

    fn drop_table(&self, database_name: &str, table_name: &str) -> Result<(), CatalogError>;

    fn rename_table(&self, request: RenameTableRequest) -> Result<TableCatalogEntry, CatalogError>;

    fn alter_table(&self, request: AlterTableRequest) -> Result<TableCatalogEntry, CatalogError>;
}

type PaimonCatalogLoader = dyn Fn() -> Result<Arc<dyn PaimonCatalog>, CatalogError> + Send + Sync;

#[derive(Default, Clone)]
pub struct CatalogRegistry {
    catalogs: Arc<RwLock<BTreeMap<String, Arc<dyn Catalog>>>>,
}

impl CatalogRegistry {
    pub fn register(&self, catalog: Arc<dyn Catalog>) {
        self.catalogs
            .write()
            .expect("catalog registry lock poisoned")
            .insert(catalog.entry().path.catalog().to_owned(), catalog);
    }

    pub fn get(&self, catalog_name: &str) -> Option<Arc<dyn Catalog>> {
        self.catalogs
            .read()
            .expect("catalog registry lock poisoned")
            .get(catalog_name)
            .cloned()
    }
}

struct ManagedPaimonRuntime {
    catalog_loader: Arc<PaimonCatalogLoader>,
    catalog: OnceLock<Arc<dyn PaimonCatalog>>,
    executor: OnceLock<tokio::runtime::Runtime>,
}

impl ManagedPaimonRuntime {
    fn new(config: &CatalogConfig) -> Self {
        let options = config.paimon_options();
        Self {
            catalog_loader: Arc::new(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime for paimon catalog loader must build");
                runtime
                    .block_on(PaimonCatalogFactory::create(options.clone()))
                    .map_err(|error| CatalogError::CatalogBackend {
                        backend: "paimon",
                        message: error.to_string(),
                    })
            }),
            catalog: OnceLock::new(),
            executor: OnceLock::new(),
        }
    }

    #[cfg(test)]
    fn with_catalog_loader(
        catalog_loader: impl Fn() -> Result<Arc<dyn PaimonCatalog>, CatalogError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            catalog_loader: Arc::new(catalog_loader),
            catalog: OnceLock::new(),
            executor: OnceLock::new(),
        }
    }

    fn executor(&self) -> &tokio::runtime::Runtime {
        self.executor.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime for paimon catalog must build")
        })
    }

    fn catalog(&self) -> Result<Arc<dyn PaimonCatalog>, CatalogError> {
        if let Some(catalog) = self.catalog.get() {
            return Ok(catalog.clone());
        }

        let catalog = (self.catalog_loader)()?;
        let _ = self.catalog.set(catalog.clone());
        Ok(self
            .catalog
            .get()
            .expect("paimon catalog cache must be initialized")
            .clone())
    }

    #[allow(dead_code)]
    fn get_database(&self, database_name: &str) -> Result<PaimonDatabase, CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.get_database(database_name))
            .map_err(|error| map_paimon_error("paimon", error))
    }

    fn load_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<PaimonTable, CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.get_table(&Identifier::new(database_name, table_name)))
            .map_err(|error| map_paimon_error("paimon", error))
    }

    fn create_database(&self, database_name: &str) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.create_database(database_name, false, HashMap::new()))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }

    fn create_table(&self, request: &CreateTableRequest) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        let schema = PaimonSchemaAdapter::build_schema(request)?;
        self.executor()
            .block_on(catalog.create_table(
                &Identifier::new(&request.database_name, &request.table_name),
                schema,
                false,
            ))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }

    fn drop_database(&self, database_name: &str) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.drop_database(database_name, false, false))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }

    fn drop_table(&self, database_name: &str, table_name: &str) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.drop_table(&Identifier::new(database_name, table_name), false))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }

    fn rename_table(&self, request: &RenameTableRequest) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        self.executor()
            .block_on(catalog.rename_table(
                &Identifier::new(&request.database_name, &request.table_name),
                &Identifier::new(&request.new_database_name, &request.new_table_name),
                false,
            ))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }

    fn alter_table(&self, request: &AlterTableRequest) -> Result<(), CatalogError> {
        let catalog = self.catalog()?;
        let changes = request
            .operations
            .iter()
            .map(map_alter_operation)
            .collect::<Result<Vec<_>, _>>()?;
        self.executor()
            .block_on(catalog.alter_table(
                &Identifier::new(&request.database_name, &request.table_name),
                changes,
                false,
            ))
            .map_err(|error| map_paimon_write_error("paimon", error))
    }
}

#[derive(Clone)]
pub struct ManagedPaimonCatalog {
    entry: CatalogEntry,
    store: CatalogStore,
    paimon_runtime: Arc<ManagedPaimonRuntime>,
}

impl ManagedPaimonCatalog {
    pub fn new(entry: CatalogEntry, store: CatalogStore, config: &CatalogConfig) -> Self {
        Self {
            entry,
            store,
            paimon_runtime: Arc::new(ManagedPaimonRuntime::new(config)),
        }
    }

    #[cfg(test)]
    fn with_catalog_loader(
        entry: CatalogEntry,
        store: CatalogStore,
        catalog_loader: impl Fn() -> Result<Arc<dyn PaimonCatalog>, CatalogError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            entry,
            store,
            paimon_runtime: Arc::new(ManagedPaimonRuntime::with_catalog_loader(catalog_loader)),
        }
    }

    fn require_database_path(&self, database_name: &str) -> Result<DatabasePath, CatalogError> {
        DatabasePath::new(self.entry.path.catalog(), database_name)
    }

    fn require_table_path(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<TablePath, CatalogError> {
        TablePath::new(self.entry.path.catalog(), database_name, table_name)
    }

    fn require_database(&self, database_name: &str) -> Result<DatabaseCatalogEntry, CatalogError> {
        let path = self.require_database_path(database_name)?;
        self.store
            .get_database(&path)?
            .ok_or_else(|| CatalogError::DatabaseNotFound {
                catalog: self.entry.path.catalog().to_owned(),
                database: database_name.to_owned(),
            })
    }

    fn require_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let path = self.require_table_path(database_name, table_name)?;
        self.store
            .get_table(&path)?
            .ok_or_else(|| CatalogError::TableNotFound {
                catalog: self.entry.path.catalog().to_owned(),
                database: database_name.to_owned(),
                table: table_name.to_owned(),
            })
    }

    fn build_table_entry(
        &self,
        database: &DatabaseCatalogEntry,
        table_id: uuid::Uuid,
        path: TablePath,
        table: &PaimonTable,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let schema = PaimonSchemaAdapter::table_schema_to_brewdb(table.schema())?;
        let table_options = map_paimon_table_options(table.schema());
        Ok(TableCatalogEntry::new(
            table_id,
            self.entry.catalog_id,
            database.database_id,
            path,
            schema,
            table.location(),
            self.entry.lake_format_kind,
            self.entry.mode,
        )
        .with_options(table_options))
    }
}

impl Catalog for ManagedPaimonCatalog {
    fn entry(&self) -> &CatalogEntry {
        &self.entry
    }

    fn get_database(&self, database_name: &str) -> Result<DatabaseCatalogEntry, CatalogError> {
        self.require_database(database_name)
    }

    fn get_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<TableCatalogEntry, CatalogError> {
        self.require_table(database_name, table_name)
    }

    fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<DatabaseCatalogEntry, CatalogError> {
        let path = self.require_database_path(&request.database_name)?;
        if self.store.database_exists(&path)? {
            return Err(CatalogError::DuplicateDatabase {
                catalog: self.entry.path.catalog().to_owned(),
                database: request.database_name,
            });
        }

        self.paimon_runtime.create_database(path.database())?;
        let entry = DatabaseCatalogEntry::new(uuid::Uuid::new_v4(), self.entry.catalog_id, path)
            .with_options(request.database_options);
        self.store.create_database(entry.clone())?;
        Ok(entry)
    }

    fn create_table(&self, request: CreateTableRequest) -> Result<TableCatalogEntry, CatalogError> {
        if self.entry.lake_format_kind != LakeFormatKind::Paimon {
            return Err(CatalogError::CatalogFormatMismatch {
                catalog: self.entry.path.catalog().to_owned(),
                expected: LakeFormatKind::Paimon.as_str(),
                actual: self.entry.lake_format_kind.as_str(),
            });
        }

        let database = self.require_database(&request.database_name)?;
        let table_path = self.require_table_path(&request.database_name, &request.table_name)?;
        if self.store.table_exists(&table_path)? {
            return Err(CatalogError::DuplicateTable {
                catalog: self.entry.path.catalog().to_owned(),
                database: request.database_name,
                table: request.table_name,
            });
        }

        self.paimon_runtime.create_table(&request)?;
        let live_table = self
            .paimon_runtime
            .load_table(table_path.database(), table_path.table())?;
        let entry =
            self.build_table_entry(&database, uuid::Uuid::new_v4(), table_path, &live_table)?;
        self.store.create_table(entry.clone())?;
        Ok(entry)
    }

    fn drop_database(&self, database_name: &str) -> Result<(), CatalogError> {
        let path = self.require_database_path(database_name)?;
        if !self.store.database_exists(&path)? {
            return Err(CatalogError::DatabaseNotFound {
                catalog: self.entry.path.catalog().to_owned(),
                database: database_name.to_owned(),
            });
        }

        self.paimon_runtime.drop_database(database_name)?;
        self.store.delete_database(&path)?;
        Ok(())
    }

    fn drop_table(&self, database_name: &str, table_name: &str) -> Result<(), CatalogError> {
        let path = self.require_table_path(database_name, table_name)?;
        if !self.store.table_exists(&path)? {
            return Err(CatalogError::TableNotFound {
                catalog: self.entry.path.catalog().to_owned(),
                database: database_name.to_owned(),
                table: table_name.to_owned(),
            });
        }

        self.paimon_runtime.drop_table(database_name, table_name)?;
        self.store.delete_table(&path)?;
        Ok(())
    }

    fn rename_table(&self, request: RenameTableRequest) -> Result<TableCatalogEntry, CatalogError> {
        let current = self.require_table(&request.database_name, &request.table_name)?;
        let target_database = self.require_database(&request.new_database_name)?;
        let target_path =
            self.require_table_path(&request.new_database_name, &request.new_table_name)?;
        if self.store.table_exists(&target_path)? {
            return Err(CatalogError::DuplicateTable {
                catalog: self.entry.path.catalog().to_owned(),
                database: request.new_database_name,
                table: request.new_table_name,
            });
        }

        self.paimon_runtime.rename_table(&request)?;
        let live_table = self
            .paimon_runtime
            .load_table(target_path.database(), target_path.table())?;
        let updated =
            self.build_table_entry(&target_database, current.table_id, target_path, &live_table)?;
        self.store.delete_table(&current.path)?;
        self.store.create_table(updated.clone())?;
        Ok(updated)
    }

    fn alter_table(&self, request: AlterTableRequest) -> Result<TableCatalogEntry, CatalogError> {
        let current = self.require_table(&request.database_name, &request.table_name)?;
        let database = self.require_database(&request.database_name)?;
        self.paimon_runtime.alter_table(&request)?;
        let live_table = self
            .paimon_runtime
            .load_table(&request.database_name, &request.table_name)?;
        let updated =
            self.build_table_entry(&database, current.table_id, current.path, &live_table)?;
        self.store.update_table(updated.clone())?;
        Ok(updated)
    }
}

fn map_paimon_error(backend: &'static str, error: paimon::Error) -> CatalogError {
    match error {
        paimon::Error::DatabaseNotExist { database } => CatalogError::DatabaseNotFound {
            catalog: "<paimon>".to_owned(),
            database,
        },
        paimon::Error::TableNotExist { full_name } => {
            let mut parts = full_name.split('.');
            CatalogError::TableNotFound {
                catalog: "<paimon>".to_owned(),
                database: parts.next().unwrap_or_default().to_owned(),
                table: parts.next().unwrap_or_default().to_owned(),
            }
        }
        other => CatalogError::CatalogBackend {
            backend,
            message: other.to_string(),
        },
    }
}

fn map_paimon_write_error(backend: &'static str, error: paimon::Error) -> CatalogError {
    match error {
        paimon::Error::DatabaseAlreadyExist { database } => CatalogError::DuplicateDatabase {
            catalog: "<paimon>".to_owned(),
            database,
        },
        paimon::Error::TableAlreadyExist { full_name } => {
            let mut parts = full_name.split('.');
            CatalogError::DuplicateTable {
                catalog: "<paimon>".to_owned(),
                database: parts.next().unwrap_or_default().to_owned(),
                table: parts.next().unwrap_or_default().to_owned(),
            }
        }
        other => map_paimon_error(backend, other),
    }
}

fn map_alter_operation(operation: &AlterTableOperation) -> Result<SchemaChange, CatalogError> {
    PaimonSchemaAdapter::alter_operation_to_change(operation)
}

fn map_paimon_table_options(schema: &paimon::spec::TableSchema) -> BTreeMap<String, String> {
    schema
        .options()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use paimon::catalog::{Catalog as PaimonCatalog, Database as PaimonDatabase, Identifier};
    use paimon::io::FileIOBuilder;
    use paimon::spec::{Schema, TableSchema as PaimonTableSchema};
    use paimon::table::Table as PaimonTable;

    use crate::backend::CatalogStore;
    use crate::config::CatalogConfig;
    use crate::errors::CatalogError;
    use crate::model::{CatalogEntry, CatalogMode, LakeFormatKind};
    use crate::path::CatalogPath;
    use crate::requests::{
        AlterTableOperation, AlterTableRequest, CreateDatabaseRequest, CreateTableRequest,
        RenameTableRequest,
    };
    use crate::store::memory::MemoryCatalogStoreBackend;

    use super::{Catalog, ManagedPaimonCatalog};

    struct MockPaimonCatalog {
        database: PaimonDatabase,
        table: PaimonTable,
    }

    #[async_trait]
    impl PaimonCatalog for MockPaimonCatalog {
        async fn list_databases(&self) -> paimon::Result<Vec<String>> {
            Ok(vec![self.database.name.clone()])
        }

        async fn create_database(
            &self,
            _name: &str,
            _ignore_if_exists: bool,
            _properties: HashMap<String, String>,
        ) -> paimon::Result<()> {
            Ok(())
        }

        async fn get_database(&self, _name: &str) -> paimon::Result<PaimonDatabase> {
            Ok(self.database.clone())
        }

        async fn drop_database(
            &self,
            _name: &str,
            _ignore_if_not_exists: bool,
            _cascade: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }

        async fn get_table(&self, _identifier: &Identifier) -> paimon::Result<PaimonTable> {
            Ok(self.table.clone())
        }

        async fn list_tables(&self, _database_name: &str) -> paimon::Result<Vec<String>> {
            Ok(vec![self.table.identifier().object().to_owned()])
        }

        async fn create_table(
            &self,
            _identifier: &Identifier,
            _creation: Schema,
            _ignore_if_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }

        async fn drop_table(
            &self,
            _identifier: &Identifier,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }

        async fn rename_table(
            &self,
            _from: &Identifier,
            _to: &Identifier,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }

        async fn alter_table(
            &self,
            _identifier: &Identifier,
            _changes: Vec<paimon::spec::SchemaChange>,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
    }

    fn mock_catalog() -> ManagedPaimonCatalog {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let store = CatalogStore::new(backend);
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            LakeFormatKind::Paimon,
        );

        let schema = Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .option("bucket", "1")
            .build()
            .unwrap();
        let table_schema = PaimonTableSchema::new(1, &schema);
        let table = PaimonTable::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("sales", "orders"),
            "s3://warehouse/sales/orders".to_owned(),
            table_schema,
            None,
        );
        let database = PaimonDatabase {
            name: "sales".to_owned(),
            options: HashMap::new(),
            comment: None,
        };

        ManagedPaimonCatalog::with_catalog_loader(entry, store, move || {
            Ok(Arc::new(MockPaimonCatalog {
                database: database.clone(),
                table: table.clone(),
            }))
        })
    }

    #[test]
    fn managed_paimon_catalog_creates_entries_in_store() {
        let catalog = mock_catalog();

        let database = catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let table = catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        assert_eq!(database.path.to_string(), "prod.sales");
        assert_eq!(table.path.to_string(), "prod.sales.orders");
        assert_eq!(table.table_location, "s3://warehouse/sales/orders");
        assert_eq!(table.table_schema.fields.len(), 1);
        assert_eq!(
            table.table_options.get("bucket").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn managed_paimon_catalog_renames_and_alters_using_store_identity() {
        let catalog = mock_catalog();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let created = catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

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

        assert_eq!(renamed.table_id, created.table_id);
        assert_eq!(renamed.path.to_string(), "prod.sales.orders_v2");
        assert_eq!(altered.table_id, created.table_id);
        assert_eq!(altered.path.to_string(), "prod.sales.orders_v2");
    }

    #[test]
    fn managed_paimon_catalog_rejects_missing_store_entries() {
        let catalog = mock_catalog();

        let error = catalog.get_table("sales", "orders").unwrap_err();

        assert_eq!(
            error,
            CatalogError::TableNotFound {
                catalog: "prod".to_owned(),
                database: "sales".to_owned(),
                table: "orders".to_owned(),
            }
        );
    }

    #[test]
    fn managed_paimon_catalog_builds_provider_from_config() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let store = CatalogStore::new(backend);
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            LakeFormatKind::Paimon,
        );
        let config = CatalogConfig {
            store_backend: crate::config::CatalogStoreBackendKind::Memory,
            paimon_warehouse: "memory:/warehouse".to_owned(),
        };

        let catalog = ManagedPaimonCatalog::new(entry, store, &config);

        assert_eq!(catalog.entry().path.catalog(), "prod");
    }
}
