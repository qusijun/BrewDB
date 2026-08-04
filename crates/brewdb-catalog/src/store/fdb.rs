//! FoundationDB catalog store skeleton.

use crate::backend::CatalogStoreBackend;
use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseCatalogEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbCatalogStoreOptions {
    pub cluster_file: Option<String>,
    pub root_path: String,
}

impl Default for FdbCatalogStoreOptions {
    fn default() -> Self {
        Self {
            cluster_file: None,
            root_path: "/brewdb/catalog".to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FdbCatalogStoreBackend {
    options: FdbCatalogStoreOptions,
}

impl FdbCatalogStoreBackend {
    pub fn new(options: FdbCatalogStoreOptions) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &FdbCatalogStoreOptions {
        &self.options
    }

    fn not_implemented() -> CatalogError {
        CatalogError::BackendNotImplemented { backend: "fdb" }
    }
}

impl CatalogStoreBackend for FdbCatalogStoreBackend {
    fn get_catalog(&self, _path: &CatalogPath) -> Result<Option<CatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_catalog_by_ref(
        &self,
        _catalog_ref: CatalogRef,
    ) -> Result<Option<CatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_database(
        &self,
        _path: &DatabasePath,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_database_by_ref(
        &self,
        _database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_table(&self, _path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_table_by_ref(
        &self,
        _table_ref: TableRef,
    ) -> Result<Option<TableCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn create_catalog(&self, _entry: CatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn create_database(&self, _entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn create_table(&self, _entry: TableCatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn update_catalog(&self, _entry: CatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn update_database(&self, _entry: DatabaseCatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn update_table(&self, _entry: TableCatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn delete_catalog(&self, _path: &CatalogPath) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn delete_database(&self, _path: &DatabasePath) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn delete_table(&self, _path: &TablePath) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn list_databases(
        &self,
        _catalog_path: &CatalogPath,
    ) -> Result<Vec<DatabaseCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn list_tables(
        &self,
        _database_path: &DatabasePath,
    ) -> Result<Vec<TableCatalogEntry>, CatalogError> {
        Err(Self::not_implemented())
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::CatalogStoreBackend;
    use crate::errors::CatalogError;
    use crate::path::CatalogPath;

    use super::{FdbCatalogStoreBackend, FdbCatalogStoreOptions};

    #[test]
    fn fdb_backend_preserves_options() {
        let backend = FdbCatalogStoreBackend::new(FdbCatalogStoreOptions {
            cluster_file: Some("/etc/foundationdb/fdb.cluster".to_owned()),
            root_path: "/brewdb/test-catalog".to_owned(),
        });

        assert_eq!(
            backend.options(),
            &FdbCatalogStoreOptions {
                cluster_file: Some("/etc/foundationdb/fdb.cluster".to_owned()),
                root_path: "/brewdb/test-catalog".to_owned(),
            }
        );
    }

    #[test]
    fn fdb_backend_is_explicitly_unimplemented_for_now() {
        let backend = FdbCatalogStoreBackend::new(FdbCatalogStoreOptions::default());

        let error = backend
            .get_catalog(&CatalogPath::new("prod").unwrap())
            .unwrap_err();

        assert_eq!(
            error,
            CatalogError::BackendNotImplemented { backend: "fdb" }
        );
    }
}
