//! FoundationDB catalog store skeleton.

use crate::backend::CatalogStoreBackend;
use crate::errors::CatalogError;
use crate::model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, TableCatalogEntry, TableRef,
};
use crate::path::{CatalogPath, DatabasePath, TablePath};

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct FdbCatalogStoreBackend {
    options: FdbCatalogStoreOptions,
}

impl FdbCatalogStoreBackend {
    #[allow(dead_code)]
    pub fn new(options: FdbCatalogStoreOptions) -> Self {
        Self { options }
    }

    #[allow(dead_code)]
    pub fn options(&self) -> &FdbCatalogStoreOptions {
        &self.options
    }

    #[allow(dead_code)]
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

    fn get_database(&self, _path: &DatabasePath) -> Result<Option<DatabaseEntry>, CatalogError> {
        Err(Self::not_implemented())
    }

    fn get_database_by_ref(
        &self,
        _database_ref: DatabaseRef,
    ) -> Result<Option<DatabaseEntry>, CatalogError> {
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

    fn put_catalog(&self, _entry: CatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn put_database(&self, _entry: DatabaseEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn put_table(&self, _entry: TableCatalogEntry) -> Result<(), CatalogError> {
        Err(Self::not_implemented())
    }

    fn delete_table(&self, _path: &TablePath) -> Result<Option<TableCatalogEntry>, CatalogError> {
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
