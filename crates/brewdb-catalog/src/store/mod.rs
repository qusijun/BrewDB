//! Catalog store backend implementations.

use std::sync::Arc;

use crate::catalog::backend::CatalogStore;
use crate::catalog::config::{CatalogConfig, CatalogStoreBackendKind};

pub mod fdb;
pub mod memory;
pub mod rocksdb;

pub fn open_catalog_store(config: &CatalogConfig) -> CatalogStore {
    match config.store_backend {
        CatalogStoreBackendKind::Fdb => CatalogStore::new(Arc::new(
            fdb::FdbCatalogStoreBackend::new(fdb::FdbCatalogStoreOptions::default()),
        )),
        CatalogStoreBackendKind::Memory => {
            CatalogStore::new(Arc::new(memory::MemoryCatalogStoreBackend::default()))
        }
        CatalogStoreBackendKind::Rocksdb => CatalogStore::new(Arc::new(
            rocksdb::RocksdbCatalogStoreBackend::new(config.rocksdb_options())
                .expect("rocksdb catalog store should open from catalog config"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::config::{CatalogConfig, CatalogStoreBackendKind};
    use crate::catalog::errors::CatalogError;
    use crate::catalog::path::CatalogPath;

    use super::open_catalog_store;

    #[test]
    fn store_factory_opens_memory_backend() {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Memory,
            paimon_warehouse: String::new(),
            rocksdb_datadir: String::new(),
            rocksdb_root: String::new(),
        });

        assert!(
            store
                .get_catalog(&CatalogPath::new("prod").unwrap())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn store_factory_opens_fdb_skeleton_backend() {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Fdb,
            paimon_warehouse: String::new(),
            rocksdb_datadir: String::new(),
            rocksdb_root: String::new(),
        });

        let error = store
            .get_catalog(&CatalogPath::new("prod").unwrap())
            .unwrap_err();

        assert_eq!(
            error,
            CatalogError::BackendNotImplemented { backend: "fdb" }
        );
    }

    #[test]
    fn store_factory_opens_rocksdb_backend() {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Rocksdb,
            paimon_warehouse: String::new(),
            rocksdb_datadir: String::new(),
            rocksdb_root: String::new(),
        });

        assert!(
            store
                .get_catalog(&CatalogPath::new("prod").unwrap())
                .unwrap()
                .is_none()
        );
    }
}
