//! RocksDB-backed catalog store using OpenDAL.

use std::path::PathBuf;
use std::sync::Arc;

use opendal::ErrorKind;
use opendal::blocking;
use opendal::services::Rocksdb;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::catalog::backend::CatalogStoreBackend;
use crate::catalog::errors::CatalogError;
use crate::catalog::model::{CatalogEntry, DatabaseCatalogEntry, TableCatalogEntry};
use crate::catalog::path::{CatalogPath, DatabasePath, TablePath};

const ROOT: &str = "/brewdb/catalog";
const CATALOG_PATH_PREFIX: &str = "catalogs/by-path/";
const CATALOG_ID_PREFIX: &str = "catalogs/by-id/";
const DATABASE_PATH_PREFIX: &str = "databases/by-path/";
const DATABASE_ID_PREFIX: &str = "databases/by-id/";
const TABLE_PATH_PREFIX: &str = "tables/by-path/";
const TABLE_ID_PREFIX: &str = "tables/by-id/";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RocksdbCatalogStoreOptions {
    pub datadir: Option<PathBuf>,
    pub root: String,
}

impl Default for RocksdbCatalogStoreOptions {
    fn default() -> Self {
        Self {
            datadir: None,
            root: ROOT.to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct RocksdbCatalogStoreBackend {
    _runtime: Arc<tokio::runtime::Runtime>,
    operator: blocking::Operator,
}

impl RocksdbCatalogStoreBackend {
    pub fn new(options: RocksdbCatalogStoreOptions) -> Result<Self, CatalogError> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|error| CatalogError::CatalogBackend {
                    backend: "rocksdb",
                    message: error.to_string(),
                })?,
        );

        let datadir = options.datadir.unwrap_or_else(|| {
            std::env::temp_dir().join(format!("brewdb-catalog-{}", uuid::Uuid::new_v4()))
        });
        let builder = Rocksdb::default()
            .datadir(datadir.to_string_lossy().as_ref())
            .root(&options.root);

        let operator = {
            let _guard = runtime.enter();
            let operator = opendal::Operator::new(builder)
                .map_err(|error| map_open_dal_error("rocksdb", error))?
                .finish();
            blocking::Operator::new(operator)
                .map_err(|error| map_open_dal_error("rocksdb", error))?
        };

        Ok(Self {
            _runtime: runtime,
            operator,
        })
    }

    fn read_record<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, CatalogError> {
        match self.operator.read(key) {
            Ok(data) => serde_json::from_slice(&data.to_vec())
                .map(Some)
                .map_err(|error| CatalogError::CatalogBackend {
                    backend: "rocksdb",
                    message: error.to_string(),
                }),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(map_open_dal_error("rocksdb", error)),
        }
    }

    fn write_record<T: Serialize>(&self, key: &str, entry: &T) -> Result<(), CatalogError> {
        let data = serde_json::to_vec(entry).map_err(|error| CatalogError::CatalogBackend {
            backend: "rocksdb",
            message: error.to_string(),
        })?;
        self.operator
            .write(key, data)
            .map_err(|error| map_open_dal_error("rocksdb", error))?;
        Ok(())
    }

    fn delete_key(&self, key: &str) -> Result<(), CatalogError> {
        match self.operator.delete(key) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(map_open_dal_error("rocksdb", error)),
        }
    }

    fn list_records<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<T>, CatalogError> {
        let mut entries = Vec::new();
        for entry in self
            .operator
            .list(prefix)
            .map_err(|error| map_open_dal_error("rocksdb", error))?
        {
            let Some(value) = self.read_record::<T>(entry.path())? else {
                continue;
            };
            entries.push(value);
        }
        Ok(entries)
    }

    fn key_for_catalog_path(path: &CatalogPath) -> String {
        format!("{CATALOG_PATH_PREFIX}{}", path.catalog())
    }

    fn key_for_catalog_id(catalog_id: uuid::Uuid) -> String {
        format!("{CATALOG_ID_PREFIX}{catalog_id}")
    }

    fn key_for_database_path(path: &DatabasePath) -> String {
        format!("{DATABASE_PATH_PREFIX}{}", path)
    }

    fn key_for_database_id(database_id: uuid::Uuid) -> String {
        format!("{DATABASE_ID_PREFIX}{database_id}")
    }

    fn key_for_table_path(path: &TablePath) -> String {
        format!("{TABLE_PATH_PREFIX}{}", path)
    }

    fn key_for_table_id(table_id: uuid::Uuid) -> String {
        format!("{TABLE_ID_PREFIX}{table_id}")
    }
}

impl CatalogStoreBackend for RocksdbCatalogStoreBackend {
    fn get_catalog(&self, path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_catalog_path(path))
    }

    fn get_catalog_by_ref(
        &self,
        catalog_ref: crate::catalog::model::CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_catalog_id(catalog_ref.id()))
    }

    fn get_database(
        &self,
        path: &DatabasePath,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_database_path(path))
    }

    fn get_database_by_ref(
        &self,
        database_ref: crate::catalog::model::DatabaseRef,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_database_id(database_ref.id()))
    }

    fn get_table(&self, path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_table_path(path))
    }

    fn get_table_by_ref(
        &self,
        table_ref: crate::catalog::model::TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError> {
        self.read_record(&Self::key_for_table_id(table_ref.id()))
    }

    fn create_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        self.write_record(&Self::key_for_catalog_id(entry.catalog_id), &entry)?;
        self.write_record(&Self::key_for_catalog_path(&entry.path), &entry)?;
        Ok(())
    }

    fn create_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        self.write_record(&Self::key_for_database_id(entry.database_id), &entry)?;
        self.write_record(&Self::key_for_database_path(&entry.path), &entry)?;
        Ok(())
    }

    fn create_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        self.write_record(&Self::key_for_table_id(entry.table_id), &entry)?;
        self.write_record(&Self::key_for_table_path(&entry.path), &entry)?;
        Ok(())
    }

    fn update_catalog(&self, entry: CatalogEntry) -> Result<(), CatalogError> {
        if let Some(existing) = self.get_catalog_by_ref(entry.catalog_ref())?
            && existing.path != entry.path
        {
            self.delete_key(&Self::key_for_catalog_path(&existing.path))?;
        }
        self.create_catalog(entry)
    }

    fn update_database(&self, entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        if let Some(existing) = self.get_database_by_ref(entry.database_ref())?
            && existing.path != entry.path
        {
            self.delete_key(&Self::key_for_database_path(&existing.path))?;
        }
        self.create_database(entry)
    }

    fn update_table(&self, entry: TableCatalogEntry) -> Result<(), CatalogError> {
        if let Some(existing) = self.get_table_by_ref(entry.table_ref())?
            && existing.path != entry.path
        {
            self.delete_key(&Self::key_for_table_path(&existing.path))?;
        }
        self.create_table(entry)
    }

    fn delete_catalog(&self, path: &CatalogPath) -> Result<(), CatalogError> {
        if let Some(entry) = self.get_catalog(path)? {
            self.delete_key(&Self::key_for_catalog_path(path))?;
            self.delete_key(&Self::key_for_catalog_id(entry.catalog_id))?;
        }
        Ok(())
    }

    fn delete_database(&self, path: &DatabasePath) -> Result<(), CatalogError> {
        if let Some(entry) = self.get_database(path)? {
            self.delete_key(&Self::key_for_database_path(path))?;
            self.delete_key(&Self::key_for_database_id(entry.database_id))?;
        }
        Ok(())
    }

    fn delete_table(&self, path: &TablePath) -> Result<(), CatalogError> {
        if let Some(entry) = self.get_table(path)? {
            self.delete_key(&Self::key_for_table_path(path))?;
            self.delete_key(&Self::key_for_table_id(entry.table_id))?;
        }
        Ok(())
    }

    fn list_databases(
        &self,
        catalog_path: &CatalogPath,
    ) -> Result<Vec<DatabaseCatalogEntry>, CatalogError> {
        let prefix = format!("{DATABASE_PATH_PREFIX}{}.", catalog_path.catalog());
        let mut databases = self.list_records::<DatabaseCatalogEntry>(&prefix)?;
        databases.retain(|entry| entry.path.catalog() == catalog_path.catalog());
        Ok(databases)
    }

    fn list_tables(
        &self,
        database_path: &DatabasePath,
    ) -> Result<Vec<TableCatalogEntry>, CatalogError> {
        let prefix = format!("{TABLE_PATH_PREFIX}{}.", database_path.catalog());
        let mut tables = self.list_records::<TableCatalogEntry>(&prefix)?;
        tables.retain(|entry| {
            entry.path.catalog() == database_path.catalog()
                && entry.path.database() == database_path.database()
        });
        Ok(tables)
    }

    fn list_catalogs(&self) -> Result<Vec<CatalogEntry>, CatalogError> {
        self.list_records(CATALOG_PATH_PREFIX)
    }
}

fn map_open_dal_error(backend: &'static str, error: opendal::Error) -> CatalogError {
    CatalogError::CatalogBackend {
        backend,
        message: error.to_string(),
    }
}
