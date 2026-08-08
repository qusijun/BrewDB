//! Shared configuration primitives.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use crate::errors::CommonError;

const CONFIG_KEY_PREFIX: &str = "brewdb.";

pub use inventory;

#[macro_export]
macro_rules! config_definition {
    ($key:expr, $kind:expr, $default:expr) => {
        $crate::config::ConfigDefinition::new($key, $kind, $default)
    };
    ($key:expr, $kind:expr, $default:expr, [$($scope:expr),* $(,)?]) => {
        $crate::config::ConfigDefinition::new($key, $kind, $default)
            .map(|definition| definition.with_allowed_scopes([$($scope),*]))
    };
}

#[macro_export]
macro_rules! config_definitions {
    ($(($key:expr, $kind:expr, $default:expr $(, [$($scope:expr),* $(,)?])?)),* $(,)?) => {{
        vec![
            $(
                $crate::config_definition!($key, $kind, $default $(, [$($scope),*])?)
                    .expect(concat!("invalid config definition: ", stringify!($key)))
            ),*
        ]
    }};
}

#[macro_export]
macro_rules! config_registry {
    ($(($key:expr, $kind:expr, $default:expr $(, [$($scope:expr),* $(,)?])?)),* $(,)?) => {{
        let mut registry = $crate::config::ConfigRegistry::new();
        registry
            .register_definitions($crate::config_definitions!(
                $(($key, $kind, $default $(, [$($scope),*])?)),*
            ))
            .expect("duplicate config definition");
        registry
    }};
}

#[macro_export]
macro_rules! define_config_view {
    (
        $vis:vis struct $name:ident {
            $(
                $field:ident : $field_ty:ty {
                    key: $key:expr,
                    kind: $kind:ident,
                    default: $default:expr
                    $(, scopes: [$($scope:expr),* $(,)?])?
                    , parse: $parse:expr
                    $(,)?
                }
            ),* $(,)?
        }
    ) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        $vis struct $name {
            $(pub $field: $field_ty),*
        }

        impl $crate::config::ConfigView for $name {
            fn config_definitions() -> Vec<$crate::config::ConfigDefinition> {
                $crate::config_definitions!(
                    $(
                        (
                            $key,
                            $crate::config::ConfigValueKind::$kind,
                            $default
                            $(, [$($scope),*])?
                        )
                    ),*
                )
            }

            fn from_config_set(
                config: &$crate::config::ConfigSet,
            ) -> Result<Self, $crate::errors::CommonError> {
                Ok(Self {
                    $(
                        $field: ($parse)($crate::define_config_view!(
                            @extract config, $key, $kind
                        ))?
                    ),*
                })
            }
        }

        $crate::config::inventory::submit! {
            $crate::config::ConfigDefinitionSetRegistration {
                collect: <$name as $crate::config::ConfigView>::config_definitions,
            }
        }
    };
    (@extract $config:expr, $key:expr, Bool) => {
        $config.require_bool($key)?
    };
    (@extract $config:expr, $key:expr, U64) => {
        $config.require_u64($key)?
    };
    (@extract $config:expr, $key:expr, String) => {
        $config.require_string($key)?
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConfigScope {
    System,
    Session,
    Statement,
}

impl ConfigScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Session => "session",
            Self::Statement => "statement",
        }
    }

    pub const fn priority(self) -> u8 {
        match self {
            Self::System => 0,
            Self::Session => 1,
            Self::Statement => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigValueKind {
    Bool,
    U64,
    String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigValue {
    Bool(bool),
    U64(u64),
    String(String),
}

impl ConfigValue {
    pub const fn kind(&self) -> ConfigValueKind {
        match self {
            Self::Bool(_) => ConfigValueKind::Bool,
            Self::U64(_) => ConfigValueKind::U64,
            Self::String(_) => ConfigValueKind::String,
        }
    }

    pub const fn kind_name(&self) -> &'static str {
        self.kind().as_str()
    }
}

impl ConfigValueKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::U64 => "u64",
            Self::String => "string",
        }
    }
}

impl From<bool> for ConfigValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u64> for ConfigValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<String> for ConfigValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ConfigValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigDefinition {
    pub key: &'static str,
    pub value_kind: ConfigValueKind,
    pub default_value: ConfigValue,
    allowed_scopes: BTreeSet<ConfigScope>,
}

impl ConfigDefinition {
    pub fn new(
        key: &'static str,
        value_kind: ConfigValueKind,
        default_value: impl Into<ConfigValue>,
    ) -> Result<Self, CommonError> {
        validate_config_key(key)?;
        let default_value = default_value.into();
        if default_value.kind() != value_kind {
            return Err(CommonError::InvalidConfiguration {
                field: key.to_owned(),
                reason: format!(
                    "default value kind mismatch: expected {}, found {}",
                    value_kind.as_str(),
                    default_value.kind_name()
                ),
            });
        }

        Ok(Self {
            key,
            value_kind,
            default_value,
            allowed_scopes: BTreeSet::from([
                ConfigScope::System,
                ConfigScope::Session,
                ConfigScope::Statement,
            ]),
        })
    }

    pub fn with_allowed_scopes(
        mut self,
        allowed_scopes: impl IntoIterator<Item = ConfigScope>,
    ) -> Self {
        self.allowed_scopes = allowed_scopes.into_iter().collect();
        self
    }

    pub fn allows_scope(&self, scope: ConfigScope) -> bool {
        self.allowed_scopes.contains(&scope)
    }

    fn validate_value(&self, value: &ConfigValue) -> Result<(), CommonError> {
        if value.kind() != self.value_kind {
            return Err(CommonError::InvalidConfiguration {
                field: self.key.to_owned(),
                reason: format!(
                    "expected {}, found {}",
                    self.value_kind.as_str(),
                    value.kind_name()
                ),
            });
        }
        Ok(())
    }

    fn validate_scope(&self, scope: ConfigScope) -> Result<(), CommonError> {
        if !self.allows_scope(scope) {
            return Err(CommonError::InvalidConfiguration {
                field: self.key.to_owned(),
                reason: format!("scope `{}` is not allowed", scope.as_str()),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfigRegistry {
    definitions: BTreeMap<&'static str, ConfigDefinition>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn for_view<T: ConfigView>() -> Result<Self, CommonError> {
        let mut registry = Self::new();
        T::register_into(&mut registry)?;
        Ok(registry)
    }

    pub fn register(&mut self, definition: ConfigDefinition) -> Result<(), CommonError> {
        if self.definitions.contains_key(definition.key) {
            return Err(CommonError::InvalidConfiguration {
                field: definition.key.to_owned(),
                reason: "duplicate config key".to_owned(),
            });
        }
        self.definitions.insert(definition.key, definition);
        Ok(())
    }

    pub fn with_definition(mut self, definition: ConfigDefinition) -> Result<Self, CommonError> {
        self.register(definition)?;
        Ok(self)
    }

    pub fn with_view<T: ConfigView>(mut self) -> Result<Self, CommonError> {
        T::register_into(&mut self)?;
        Ok(self)
    }

    pub fn register_definitions(
        &mut self,
        definitions: impl IntoIterator<Item = ConfigDefinition>,
    ) -> Result<(), CommonError> {
        for definition in definitions {
            self.register(definition)?;
        }
        Ok(())
    }

    pub fn definition(&self, key: &str) -> Option<&ConfigDefinition> {
        self.definitions.get(key)
    }

    pub fn has_definition(&self, key: &str) -> bool {
        self.definitions.contains_key(key)
    }

    pub fn validate_patch(&self, patch: &ConfigPatch) -> Result<(), CommonError> {
        for entry in patch.entries() {
            let definition = self
                .definition(&entry.key)
                .ok_or_else(|| unknown_key_error(&entry.key))?;
            definition.validate_scope(patch.scope)?;
            definition.validate_value(&entry.value)?;
        }
        Ok(())
    }

    pub fn validate_config(&self, config: &ConfigSet) -> Result<(), CommonError> {
        for (key, value) in config.entries() {
            let definition = self.definition(key).ok_or_else(|| unknown_key_error(key))?;
            definition.validate_value(value)?;
        }
        Ok(())
    }

    pub fn entries(&self) -> impl Iterator<Item = (&'static str, &ConfigDefinition)> {
        self.definitions.iter().map(|(key, value)| (*key, value))
    }

    pub fn materialize_defaults(&self) -> ConfigSet {
        let mut values = BTreeMap::new();
        for (key, definition) in &self.definitions {
            values.insert((*key).to_owned(), definition.default_value.clone());
        }
        ConfigSet { values }
    }
}

pub trait ConfigView: Sized {
    fn config_definitions() -> Vec<ConfigDefinition>;

    fn from_config_set(config: &ConfigSet) -> Result<Self, CommonError>;

    fn register_into(registry: &mut ConfigRegistry) -> Result<(), CommonError> {
        registry.register_definitions(Self::config_definitions())
    }
}

pub struct ConfigDefinitionSetRegistration {
    pub collect: fn() -> Vec<ConfigDefinition>,
}

inventory::collect!(ConfigDefinitionSetRegistration);

pub fn global_config_registry() -> Result<ConfigRegistry, CommonError> {
    let mut registry = ConfigRegistry::new();
    for registration in inventory::iter::<ConfigDefinitionSetRegistration> {
        registry.register_definitions((registration.collect)())?;
    }
    Ok(registry)
}

#[derive(Clone, Debug)]
pub struct SystemConfigLoader {
    registry: ConfigRegistry,
}

impl SystemConfigLoader {
    pub fn new(registry: ConfigRegistry) -> Self {
        Self { registry }
    }

    pub fn for_global_registry() -> Result<Self, CommonError> {
        Ok(Self::new(global_config_registry()?))
    }

    pub fn registry(&self) -> &ConfigRegistry {
        &self.registry
    }

    pub fn load_toml_file(&self, path: impl AsRef<Path>) -> Result<ConfigSet, CommonError> {
        let path = path.as_ref();
        let contents =
            fs::read_to_string(path).map_err(|error| CommonError::InvalidConfiguration {
                field: path.display().to_string(),
                reason: format!("failed to read config file: {error}"),
            })?;
        self.load_toml_str(&contents)
    }

    pub fn load_toml_str(&self, source: &str) -> Result<ConfigSet, CommonError> {
        let value = toml::from_str::<toml::Table>(source)
            .map(toml::Value::Table)
            .map_err(|error| CommonError::InvalidConfiguration {
                field: "system_config".to_owned(),
                reason: format!("failed to parse toml: {error}"),
            })?;

        let mut patch = ConfigPatch::new(ConfigScope::System);
        flatten_toml_value(None, &value, &mut patch)?;

        let mut config = self.registry.materialize_defaults();
        config.apply_patch_with_registry(&self.registry, &patch)?;
        Ok(config)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigEntry {
    pub key: String,
    pub value: ConfigValue,
}

impl ConfigEntry {
    pub fn new(key: impl Into<String>, value: impl Into<ConfigValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigPatch {
    pub scope: ConfigScope,
    entries: Vec<ConfigEntry>,
}

impl ConfigPatch {
    pub fn new(scope: ConfigScope) -> Self {
        Self {
            scope,
            entries: Vec::new(),
        }
    }

    pub fn with_entry(mut self, key: impl Into<String>, value: impl Into<ConfigValue>) -> Self {
        self.entries.push(ConfigEntry::new(key, value));
        self
    }

    pub fn entries(&self) -> &[ConfigEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfigSet {
    values: BTreeMap<String, ConfigValue>,
}

impl ConfigSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entry(mut self, key: impl Into<String>, value: impl Into<ConfigValue>) -> Self {
        self.set(key, value);
        self
    }

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<ConfigValue>) {
        self.values.insert(key.into(), value.into());
    }

    pub fn set_with_registry(
        &mut self,
        registry: &ConfigRegistry,
        key: impl Into<String>,
        value: impl Into<ConfigValue>,
    ) -> Result<(), CommonError> {
        let key = key.into();
        let value = value.into();
        let definition = registry
            .definition(&key)
            .ok_or_else(|| unknown_key_error(&key))?;
        definition.validate_value(&value)?;
        self.values.insert(key, value);
        Ok(())
    }

    pub fn apply_patch(&mut self, patch: &ConfigPatch) {
        for entry in patch.entries() {
            self.values.insert(entry.key.clone(), entry.value.clone());
        }
    }

    pub fn apply_patch_with_registry(
        &mut self,
        registry: &ConfigRegistry,
        patch: &ConfigPatch,
    ) -> Result<(), CommonError> {
        registry.validate_patch(patch)?;
        for entry in patch.entries() {
            self.values.insert(entry.key.clone(), entry.value.clone());
        }
        Ok(())
    }

    pub fn merged(&self, patch: &ConfigPatch) -> Self {
        let mut merged = self.clone();
        merged.apply_patch(patch);
        merged
    }

    pub fn merged_with_registry(
        &self,
        registry: &ConfigRegistry,
        patch: &ConfigPatch,
    ) -> Result<Self, CommonError> {
        let mut merged = self.clone();
        merged.apply_patch_with_registry(registry, patch)?;
        Ok(merged)
    }

    pub fn merge_patches<'a>(&self, patches: impl IntoIterator<Item = &'a ConfigPatch>) -> Self {
        let mut merged = self.clone();
        let mut ordered = patches.into_iter().collect::<Vec<_>>();
        ordered.sort_by_key(|patch| patch.scope.priority());
        for patch in ordered {
            merged.apply_patch(patch);
        }
        merged
    }

    pub fn merge_patches_with_registry<'a>(
        &self,
        registry: &ConfigRegistry,
        patches: impl IntoIterator<Item = &'a ConfigPatch>,
    ) -> Result<Self, CommonError> {
        let mut merged = self.clone();
        let mut ordered = patches.into_iter().collect::<Vec<_>>();
        ordered.sort_by_key(|patch| patch.scope.priority());
        for patch in ordered {
            merged.apply_patch_with_registry(registry, patch)?;
        }
        Ok(merged)
    }

    pub fn get(&self, key: &str) -> Option<&ConfigValue> {
        self.values.get(key)
    }

    pub fn get_bool(&self, key: &str) -> Result<Option<bool>, CommonError> {
        match self.get(key) {
            Some(ConfigValue::Bool(value)) => Ok(Some(*value)),
            Some(other) => Err(type_mismatch_error(key, ConfigValueKind::Bool, other)),
            None => Ok(None),
        }
    }

    pub fn get_u64(&self, key: &str) -> Result<Option<u64>, CommonError> {
        match self.get(key) {
            Some(ConfigValue::U64(value)) => Ok(Some(*value)),
            Some(other) => Err(type_mismatch_error(key, ConfigValueKind::U64, other)),
            None => Ok(None),
        }
    }

    pub fn get_string(&self, key: &str) -> Result<Option<&str>, CommonError> {
        match self.get(key) {
            Some(ConfigValue::String(value)) => Ok(Some(value.as_str())),
            Some(other) => Err(type_mismatch_error(key, ConfigValueKind::String, other)),
            None => Ok(None),
        }
    }

    pub fn require_bool(&self, key: &str) -> Result<bool, CommonError> {
        self.get_bool(key)?.ok_or_else(|| missing_config_error(key))
    }

    pub fn require_u64(&self, key: &str) -> Result<u64, CommonError> {
        self.get_u64(key)?.ok_or_else(|| missing_config_error(key))
    }

    pub fn require_string(&self, key: &str) -> Result<&str, CommonError> {
        self.get_string(key)?
            .ok_or_else(|| missing_config_error(key))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, &ConfigValue)> {
        self.values.iter().map(|(key, value)| (key.as_str(), value))
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

fn type_mismatch_error(key: &str, expected: ConfigValueKind, actual: &ConfigValue) -> CommonError {
    CommonError::InvalidConfiguration {
        field: key.to_owned(),
        reason: format!(
            "expected {}, found {}",
            expected.as_str(),
            actual.kind_name()
        ),
    }
}

fn validate_config_key(key: &str) -> Result<(), CommonError> {
    if key.starts_with(CONFIG_KEY_PREFIX) {
        Ok(())
    } else {
        Err(CommonError::InvalidConfiguration {
            field: key.to_owned(),
            reason: format!("config key must start with `{CONFIG_KEY_PREFIX}`"),
        })
    }
}

fn unknown_key_error(key: &str) -> CommonError {
    CommonError::InvalidConfiguration {
        field: key.to_owned(),
        reason: "unknown config key".to_owned(),
    }
}

fn missing_config_error(key: &str) -> CommonError {
    CommonError::InvalidConfiguration {
        field: key.to_owned(),
        reason: "missing required config value".to_owned(),
    }
}

fn flatten_toml_value(
    prefix: Option<&str>,
    value: &toml::Value,
    patch: &mut ConfigPatch,
) -> Result<(), CommonError> {
    match value {
        toml::Value::Table(table) => {
            for (key, value) in table {
                let next = match prefix {
                    Some(prefix) => format!("{prefix}.{key}"),
                    None => key.clone(),
                };
                flatten_toml_value(Some(&next), value, patch)?;
            }
            Ok(())
        }
        toml::Value::Boolean(value) => {
            let key = prefix.ok_or_else(|| invalid_toml_root_error("bool"))?;
            patch.entries.push(ConfigEntry::new(key.to_owned(), *value));
            Ok(())
        }
        toml::Value::Integer(value) => {
            let key = prefix.ok_or_else(|| invalid_toml_root_error("integer"))?;
            let value = u64::try_from(*value).map_err(|_| CommonError::InvalidConfiguration {
                field: key.to_owned(),
                reason: "expected non-negative integer".to_owned(),
            })?;
            patch.entries.push(ConfigEntry::new(key.to_owned(), value));
            Ok(())
        }
        toml::Value::String(value) => {
            let key = prefix.ok_or_else(|| invalid_toml_root_error("string"))?;
            patch
                .entries
                .push(ConfigEntry::new(key.to_owned(), value.clone()));
            Ok(())
        }
        toml::Value::Float(_) => Err(invalid_toml_value_error(prefix, "float")),
        toml::Value::Array(_) => Err(invalid_toml_value_error(prefix, "array")),
        toml::Value::Datetime(_) => Err(invalid_toml_value_error(prefix, "datetime")),
    }
}

fn invalid_toml_root_error(kind: &'static str) -> CommonError {
    CommonError::InvalidConfiguration {
        field: "system_config".to_owned(),
        reason: format!("invalid root toml value kind `{kind}`"),
    }
}

fn invalid_toml_value_error(prefix: Option<&str>, kind: &'static str) -> CommonError {
    CommonError::InvalidConfiguration {
        field: prefix.unwrap_or("system_config").to_owned(),
        reason: format!("unsupported toml value kind `{kind}`"),
    }
}

#[cfg(test)]
mod tests {
    use crate::diagnostics::{DiagnosticError, ErrorCode};

    use super::{
        ConfigDefinition, ConfigPatch, ConfigRegistry, ConfigScope, ConfigSet, ConfigValue,
        ConfigValueKind, SystemConfigLoader,
    };

    crate::define_config_view! {
        struct LoaderTestConfig {
            enabled: bool {
                key: "brewdb.test.loader.enabled",
                kind: Bool,
                default: false,
                scopes: [crate::config::ConfigScope::System],
                parse: Ok,
            },
            max_threads: u64 {
                key: "brewdb.test.loader.max_threads",
                kind: U64,
                default: 8_u64,
                scopes: [crate::config::ConfigScope::System],
                parse: Ok,
            }
        }
    }

    fn test_registry() -> ConfigRegistry {
        crate::config_registry!(
            (
                "brewdb.execution.max_threads",
                ConfigValueKind::U64,
                8_u64,
                [ConfigScope::System, ConfigScope::Session]
            ),
            (
                "brewdb.execution.enable_spill",
                ConfigValueKind::Bool,
                false
            ),
            (
                "brewdb.execution.exchange_codec",
                ConfigValueKind::String,
                "arrow_ipc",
                [ConfigScope::Statement]
            ),
        )
    }

    #[test]
    fn registry_materializes_default_config() {
        let config = test_registry().materialize_defaults();

        assert_eq!(
            config.get_u64("brewdb.execution.max_threads").unwrap(),
            Some(8)
        );
        assert_eq!(
            config.get_bool("brewdb.execution.enable_spill").unwrap(),
            Some(false)
        );
        assert_eq!(
            config
                .get_string("brewdb.execution.exchange_codec")
                .unwrap(),
            Some("arrow_ipc")
        );
    }

    #[test]
    fn registry_is_the_whitelist_for_known_keys() {
        let registry = test_registry();

        assert!(registry.has_definition("brewdb.execution.max_threads"));
        assert!(!registry.has_definition("brewdb.execution.unknown_option"));
    }

    #[test]
    fn registry_validated_patch_overrides_previous_values() {
        let registry = test_registry();
        let base = registry.materialize_defaults();
        let patch = ConfigPatch::new(ConfigScope::Session)
            .with_entry("brewdb.execution.max_threads", 16_u64)
            .with_entry("brewdb.execution.enable_spill", true);

        let merged = base.merged_with_registry(&registry, &patch).unwrap();

        assert_eq!(
            merged.get_u64("brewdb.execution.max_threads").unwrap(),
            Some(16)
        );
        assert_eq!(
            merged.get_bool("brewdb.execution.enable_spill").unwrap(),
            Some(true)
        );
    }

    #[test]
    fn registry_validates_existing_config_values() {
        let registry = test_registry();
        let config = ConfigSet::new()
            .with_entry("brewdb.execution.max_threads", 32_u64)
            .with_entry("brewdb.execution.enable_spill", true);

        registry.validate_config(&config).unwrap();
    }

    #[test]
    fn higher_scope_priority_overrides_lower_scope_priority() {
        let registry = test_registry();
        let base = registry.materialize_defaults();
        let system_patch =
            ConfigPatch::new(ConfigScope::System).with_entry("brewdb.execution.max_threads", 4_u64);
        let statement_patch = ConfigPatch::new(ConfigScope::Statement)
            .with_entry("brewdb.execution.exchange_codec", "lz4_arrow_ipc");
        let session_patch = ConfigPatch::new(ConfigScope::Session)
            .with_entry("brewdb.execution.max_threads", 16_u64);

        let merged = base
            .merge_patches_with_registry(
                &registry,
                [&statement_patch, &system_patch, &session_patch],
            )
            .unwrap();

        assert_eq!(
            merged.get_u64("brewdb.execution.max_threads").unwrap(),
            Some(16)
        );
        assert_eq!(
            merged
                .get_string("brewdb.execution.exchange_codec")
                .unwrap(),
            Some("lz4_arrow_ipc")
        );
    }

    #[test]
    fn scope_priority_is_explicit() {
        assert!(ConfigScope::System.priority() < ConfigScope::Session.priority());
        assert!(ConfigScope::Session.priority() < ConfigScope::Statement.priority());
    }

    #[test]
    fn registry_rejects_invalid_config_during_validation() {
        let registry = test_registry();
        let config = ConfigSet::new().with_entry("brewdb.execution.unknown_option", true);

        let error = registry.validate_config(&config).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.unknown_option`: unknown config key"
        );
    }

    #[test]
    fn registry_rejects_unknown_key() {
        let registry = test_registry();
        let mut config = registry.materialize_defaults();
        let patch = ConfigPatch::new(ConfigScope::Session)
            .with_entry("brewdb.execution.unknown_option", true);

        let error = config
            .apply_patch_with_registry(&registry, &patch)
            .unwrap_err();

        assert_eq!(error.error_code(), ErrorCode::INVALID_CONFIGURATION);
        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.unknown_option`: unknown config key"
        );
    }

    #[test]
    fn registry_rejects_scope_violation() {
        let registry = test_registry();
        let mut config = registry.materialize_defaults();
        let patch = ConfigPatch::new(ConfigScope::Session)
            .with_entry("brewdb.execution.exchange_codec", "lz4_arrow_ipc");

        let error = config
            .apply_patch_with_registry(&registry, &patch)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.exchange_codec`: scope `session` is not allowed"
        );
    }

    #[test]
    fn set_with_registry_rejects_unknown_key() {
        let registry = test_registry();
        let mut config = ConfigSet::new();

        let error = config
            .set_with_registry(&registry, "brewdb.execution.unknown_option", true)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.unknown_option`: unknown config key"
        );
    }

    #[test]
    fn typed_access_rejects_wrong_value_kind() {
        let config = ConfigSet::new().with_entry("brewdb.execution.max_threads", "eight");

        let error = config.get_u64("brewdb.execution.max_threads").unwrap_err();

        assert_eq!(error.error_code(), ErrorCode::INVALID_CONFIGURATION);
        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.max_threads`: expected u64, found string"
        );
    }

    #[test]
    fn registry_rejects_default_value_kind_mismatch() {
        let error = ConfigDefinition::new(
            "brewdb.execution.max_threads",
            ConfigValueKind::U64,
            "eight",
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.execution.max_threads`: default value kind mismatch: expected u64, found string"
        );
    }

    #[test]
    fn definitions_require_brewdb_prefix() {
        let error = ConfigDefinition::new("execution.max_threads", ConfigValueKind::U64, 8_u64)
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `execution.max_threads`: config key must start with `brewdb.`"
        );
    }

    #[test]
    fn config_entries_iterate_in_key_order() {
        let config = ConfigSet::new()
            .with_entry("b.key", true)
            .with_entry("a.key", ConfigValue::U64(1));

        let keys = config.entries().map(|(key, _)| key).collect::<Vec<_>>();

        assert_eq!(keys, vec!["a.key", "b.key"]);
    }

    #[test]
    fn registry_macro_registers_literal_whitelist() {
        let registry = crate::config_registry!(
            (
                "brewdb.runtime.task_slots",
                ConfigValueKind::U64,
                32_u64,
                [ConfigScope::System]
            ),
            (
                "brewdb.execution.enable_adaptive_spill",
                ConfigValueKind::Bool,
                false
            ),
        );

        assert!(registry.has_definition("brewdb.runtime.task_slots"));
        assert!(registry.has_definition("brewdb.execution.enable_adaptive_spill"));
    }

    #[test]
    fn system_config_loader_applies_system_toml_over_defaults() {
        let registry = ConfigRegistry::for_view::<LoaderTestConfig>().unwrap();
        let loader = SystemConfigLoader::new(registry);

        let config = loader
            .load_toml_str(
                r#"
                brewdb.test.loader.enabled = true
                brewdb.test.loader.max_threads = 32
                "#,
            )
            .unwrap();

        assert_eq!(
            config.require_bool("brewdb.test.loader.enabled").unwrap(),
            true
        );
        assert_eq!(
            config
                .require_u64("brewdb.test.loader.max_threads")
                .unwrap(),
            32
        );
    }

    #[test]
    fn system_config_loader_rejects_unknown_key() {
        let registry = ConfigRegistry::for_view::<LoaderTestConfig>().unwrap();
        let loader = SystemConfigLoader::new(registry);

        let error = loader
            .load_toml_str(
                r#"
                brewdb.test.loader.unknown = true
                "#,
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid configuration for `brewdb.test.loader.unknown`: unknown config key"
        );
    }
}
