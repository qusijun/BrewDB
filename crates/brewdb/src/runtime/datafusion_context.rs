//! DataFusion context adapters for BrewDB query context.

use std::collections::HashMap;
use std::sync::Arc;

use crate::common::config::{ConfigSet, ConfigValue};
use crate::common::context::QueryContext;
use datafusion::prelude::{SessionConfig, SessionContext as DataFusionSessionContext};
use datafusion_common::Result as DataFusionResult;
use datafusion_optimizer::OptimizerContext;

const DATAFUSION_CONFIG_PREFIX: &str = "datafusion.";

pub fn session_config(query_context: &QueryContext) -> DataFusionResult<SessionConfig> {
    SessionConfig::from_string_hash_map(&datafusion_settings(&query_context.settings))
}

pub fn optimizer_context(query_context: &QueryContext) -> DataFusionResult<OptimizerContext> {
    let config = session_config(query_context)?;
    Ok(OptimizerContext::new_with_config_options(Arc::new(
        config.options().as_ref().clone(),
    )))
}

pub fn session_context(query_context: &QueryContext) -> DataFusionResult<DataFusionSessionContext> {
    Ok(DataFusionSessionContext::new_with_config(session_config(
        query_context,
    )?))
}

fn datafusion_settings(settings: &ConfigSet) -> HashMap<String, String> {
    settings
        .entries()
        .filter_map(|(key, value)| {
            key.starts_with(DATAFUSION_CONFIG_PREFIX)
                .then(|| (key.to_owned(), config_value_to_string(value)))
        })
        .collect()
}

fn config_value_to_string(value: &ConfigValue) -> String {
    match value {
        ConfigValue::Bool(value) => value.to_string(),
        ConfigValue::U64(value) => value.to_string(),
        ConfigValue::String(value) => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use crate::common::config::ConfigSet;
    use crate::common::context::{QueryContext, SessionContext};
    use datafusion_optimizer::OptimizerConfig;
    use uuid::Uuid;

    #[test]
    fn session_config_uses_datafusion_settings_from_query_context() {
        let query_context = QueryContext::new(Uuid::new_v4(), SessionContext::system())
            .with_settings(
                ConfigSet::new()
                    .with_entry("datafusion.execution.batch_size", 128_u64)
                    .with_entry("brewdb.execution.max_threads", 8_u64),
            );

        let config = super::session_config(&query_context).unwrap();

        assert_eq!(config.options().as_ref().execution.batch_size, 128);
    }

    #[test]
    fn session_context_uses_datafusion_settings_from_query_context() {
        let query_context = QueryContext::new(Uuid::new_v4(), SessionContext::system())
            .with_settings(ConfigSet::new().with_entry("datafusion.execution.batch_size", 256_u64));

        let session = super::session_context(&query_context).unwrap();

        assert_eq!(
            session
                .copied_config()
                .options()
                .as_ref()
                .execution
                .batch_size,
            256
        );
    }

    #[test]
    fn optimizer_context_uses_datafusion_settings_from_query_context() {
        let query_context = QueryContext::new(Uuid::new_v4(), SessionContext::system())
            .with_settings(ConfigSet::new().with_entry("datafusion.execution.batch_size", 512_u64));

        let optimizer_context = super::optimizer_context(&query_context).unwrap();

        assert_eq!(
            optimizer_context.options().as_ref().execution.batch_size,
            512
        );
    }
}
