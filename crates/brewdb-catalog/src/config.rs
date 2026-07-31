//! Catalog configuration view definitions.

use brewdb_common::errors::CommonError;

pub const CATALOG_STORE_BACKEND_KEY: &str = "brewdb.catalog.store.backend";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogStoreBackendKind {
    Fdb,
    Memory,
}

impl CatalogStoreBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fdb => "fdb",
            Self::Memory => "memory",
        }
    }

    pub fn parse(value: &str) -> Result<Self, CommonError> {
        match value {
            "fdb" => Ok(Self::Fdb),
            "memory" => Ok(Self::Memory),
            _ => Err(CommonError::InvalidConfiguration {
                field: CATALOG_STORE_BACKEND_KEY.to_owned(),
                reason: format!("unsupported backend `{value}`, expected `fdb` or `memory`"),
            }),
        }
    }
}

brewdb_common::define_config_view! {
    pub struct CatalogConfig {
        store_backend: CatalogStoreBackendKind {
            key: CATALOG_STORE_BACKEND_KEY,
            kind: String,
            default: "fdb",
            scopes: [brewdb_common::config::ConfigScope::System],
            parse: CatalogStoreBackendKind::parse,
        }
    }
}

#[cfg(test)]
mod tests {
    use brewdb_common::config::{
        ConfigPatch, ConfigRegistry, ConfigScope, ConfigSet, ConfigView, global_config_registry,
    };

    use super::{CATALOG_STORE_BACKEND_KEY, CatalogConfig, CatalogStoreBackendKind};

    fn catalog_registry() -> ConfigRegistry {
        global_config_registry().unwrap()
    }

    #[test]
    fn catalog_config_registers_system_only_backend_key() {
        let registry = catalog_registry();
        let definition = registry.definition(CATALOG_STORE_BACKEND_KEY).unwrap();

        assert_eq!(definition.default_value, "fdb".into());
        assert!(definition.allows_scope(ConfigScope::System));
        assert!(!definition.allows_scope(ConfigScope::Session));
        assert!(!definition.allows_scope(ConfigScope::Statement));
    }

    #[test]
    fn catalog_config_decodes_backend_kind() {
        let mut config = ConfigSet::new();
        config.set(CATALOG_STORE_BACKEND_KEY, "memory");

        let catalog_config = CatalogConfig::from_config_set(&config).unwrap();

        assert_eq!(
            catalog_config.store_backend,
            CatalogStoreBackendKind::Memory
        );
    }

    #[test]
    fn catalog_config_rejects_unsupported_backend_value() {
        let mut config = ConfigSet::new();
        config.set(CATALOG_STORE_BACKEND_KEY, "rocksdb");

        let error = CatalogConfig::from_config_set(&config).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.catalog.store.backend`: unsupported backend `rocksdb`, expected `fdb` or `memory`"
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
    }
}
