//! Catalog configuration view definitions.

use paimon::{CatalogOptions as PaimonCatalogOptions, Options as PaimonOptions};

use crate::catalog::store::rocksdb::RocksdbCatalogStoreOptions;
use crate::common::errors::CommonError;

pub const CATALOG_STORE_BACKEND_KEY: &str = "brewdb.catalog.store.backend";
pub const CATALOG_PAIMON_WAREHOUSE_KEY: &str = "brewdb.catalog.paimon.warehouse";
pub const CATALOG_ROCKSDB_DATADIR_KEY: &str = "brewdb.catalog.rocksdb.datadir";
pub const CATALOG_ROCKSDB_ROOT_KEY: &str = "brewdb.catalog.rocksdb.root";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogStoreBackendKind {
    Fdb,
    Memory,
    Rocksdb,
}

impl CatalogStoreBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fdb => "fdb",
            Self::Memory => "memory",
            Self::Rocksdb => "rocksdb",
        }
    }

    pub fn parse(value: &str) -> Result<Self, CommonError> {
        match value {
            "fdb" => Ok(Self::Fdb),
            "memory" => Ok(Self::Memory),
            "rocksdb" => Ok(Self::Rocksdb),
            _ => Err(CommonError::InvalidConfiguration {
                field: CATALOG_STORE_BACKEND_KEY.to_owned(),
                reason: format!(
                    "unsupported backend `{value}`, expected `fdb`, `memory` or `rocksdb`"
                ),
            }),
        }
    }
}

brewdb_common::define_config_view! {
    pub struct CatalogConfig {
        store_backend: CatalogStoreBackendKind {
            key: CATALOG_STORE_BACKEND_KEY,
            kind: String,
            default: "memory",
            scopes: [crate::common::config::ConfigScope::System],
            parse: CatalogStoreBackendKind::parse,
        },
        paimon_warehouse: String {
            key: CATALOG_PAIMON_WAREHOUSE_KEY,
            kind: String,
            default: "",
            scopes: [crate::common::config::ConfigScope::System],
            parse: |value: &str| Ok(value.to_owned()),
        },
        rocksdb_datadir: String {
            key: CATALOG_ROCKSDB_DATADIR_KEY,
            kind: String,
            default: "",
            scopes: [crate::common::config::ConfigScope::System],
            parse: |value: &str| Ok(value.to_owned()),
        },
        rocksdb_root: String {
            key: CATALOG_ROCKSDB_ROOT_KEY,
            kind: String,
            default: "",
            scopes: [crate::common::config::ConfigScope::System],
            parse: |value: &str| Ok(value.to_owned()),
        },
    }
}

impl CatalogConfig {
    pub fn paimon_options(&self) -> PaimonOptions {
        let mut options = PaimonOptions::new();
        options.set(PaimonCatalogOptions::METASTORE, "filesystem");
        if !self.paimon_warehouse.is_empty() {
            options.set(
                PaimonCatalogOptions::WAREHOUSE,
                self.paimon_warehouse.clone(),
            );
        }
        options
    }

    pub fn rocksdb_options(&self) -> RocksdbCatalogStoreOptions {
        RocksdbCatalogStoreOptions {
            datadir: if self.rocksdb_datadir.is_empty() {
                None
            } else {
                Some(self.rocksdb_datadir.clone().into())
            },
            root: if self.rocksdb_root.is_empty() {
                "/brewdb/catalog".to_owned()
            } else {
                self.rocksdb_root.clone()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::config::{
        ConfigPatch, ConfigRegistry, ConfigScope, ConfigView, global_config_registry,
    };

    use super::{
        CATALOG_PAIMON_WAREHOUSE_KEY, CATALOG_ROCKSDB_DATADIR_KEY, CATALOG_ROCKSDB_ROOT_KEY,
        CATALOG_STORE_BACKEND_KEY, CatalogConfig, CatalogStoreBackendKind,
    };

    fn catalog_registry() -> ConfigRegistry {
        global_config_registry().unwrap()
    }

    #[test]
    fn catalog_config_registers_system_only_backend_key() {
        let registry = catalog_registry();
        let definition = registry.definition(CATALOG_STORE_BACKEND_KEY).unwrap();

        assert_eq!(definition.default_value, "memory".into());
        assert!(definition.allows_scope(ConfigScope::System));
        assert!(!definition.allows_scope(ConfigScope::Session));
        assert!(!definition.allows_scope(ConfigScope::Statement));
    }

    #[test]
    fn catalog_config_decodes_backend_kind() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry(CATALOG_STORE_BACKEND_KEY, "memory"),
            )
            .unwrap();

        let catalog_config = CatalogConfig::from_config_set(&config).unwrap();

        assert_eq!(
            catalog_config.store_backend,
            CatalogStoreBackendKind::Memory
        );
    }

    #[test]
    fn catalog_config_rejects_unsupported_backend_value() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        config.set(CATALOG_STORE_BACKEND_KEY, "sqlite");

        let error = CatalogConfig::from_config_set(&config).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.catalog.store.backend`: unsupported backend `sqlite`, expected `fdb`, `memory` or `rocksdb`"
        );
    }

    #[test]
    fn catalog_config_decodes_rocksdb_backend_kind() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry(CATALOG_STORE_BACKEND_KEY, "rocksdb"),
            )
            .unwrap();

        let catalog_config = CatalogConfig::from_config_set(&config).unwrap();

        assert_eq!(
            catalog_config.store_backend,
            CatalogStoreBackendKind::Rocksdb
        );
    }

    #[test]
    fn catalog_backend_key_rejects_non_system_scope_override() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        let patch =
            ConfigPatch::new(ConfigScope::Session).with_entry(CATALOG_STORE_BACKEND_KEY, "memory");

        let error = config
            .apply_patch_with_registry(&registry, &patch)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.catalog.store.backend`: scope `session` is not allowed"
        );
    }

    #[test]
    fn catalog_config_is_auto_registered_into_global_registry() {
        let registry = catalog_registry();

        assert!(registry.has_definition(CATALOG_STORE_BACKEND_KEY));
        assert!(registry.has_definition(CATALOG_PAIMON_WAREHOUSE_KEY));
        assert!(registry.has_definition(CATALOG_ROCKSDB_DATADIR_KEY));
        assert!(registry.has_definition(CATALOG_ROCKSDB_ROOT_KEY));
    }

    #[test]
    fn catalog_config_decodes_paimon_options() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry(CATALOG_PAIMON_WAREHOUSE_KEY, "s3://warehouse-root"),
            )
            .unwrap();

        let catalog_config = CatalogConfig::from_config_set(&config).unwrap();
        let options = catalog_config.paimon_options();

        assert_eq!(options.get("metastore"), Some(&"filesystem".to_owned()));
        assert_eq!(
            options.get("warehouse"),
            Some(&"s3://warehouse-root".to_owned())
        );
        assert_eq!(options.get("uri"), None);
    }

    #[test]
    fn catalog_config_does_not_register_removed_paimon_keys() {
        let registry = catalog_registry();

        assert!(!registry.has_definition("brewdb.catalog.paimon.metastore"));
        assert!(!registry.has_definition("brewdb.catalog.paimon.uri"));
    }

    #[test]
    fn catalog_config_decodes_rocksdb_options() {
        let registry = catalog_registry();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry(CATALOG_ROCKSDB_DATADIR_KEY, "/var/lib/brewdb/catalog")
                    .with_entry(CATALOG_ROCKSDB_ROOT_KEY, "/brewdb/catalog"),
            )
            .unwrap();

        let catalog_config = CatalogConfig::from_config_set(&config).unwrap();
        let options = catalog_config.rocksdb_options();

        assert_eq!(
            options.datadir.as_deref(),
            Some(std::path::Path::new("/var/lib/brewdb/catalog"))
        );
        assert_eq!(options.root, "/brewdb/catalog");
    }
}
