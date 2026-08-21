//! DataFusion execution context adapters for BrewDB query context.

use crate::common::config::datafusion_settings;
use crate::common::context::QueryContext;
use datafusion::prelude::{SessionConfig, SessionContext as DataFusionSessionContext};
use datafusion_common::Result as DataFusionResult;

pub fn session_config(query_context: &QueryContext) -> DataFusionResult<SessionConfig> {
    SessionConfig::from_string_hash_map(&datafusion_settings(&query_context.settings))
}

pub fn session_context(query_context: &QueryContext) -> DataFusionResult<DataFusionSessionContext> {
    Ok(DataFusionSessionContext::new_with_config(session_config(
        query_context,
    )?))
}

#[cfg(test)]
mod tests {
    use crate::common::config::ConfigSet;
    use crate::common::context::{QueryContext, SessionContext};
    use uuid::Uuid;

    #[test]
    fn session_config_uses_datafusion_settings_from_query_context() {
        let query_context = QueryContext::new(Uuid::new_v4(), SessionContext::system())
            .with_settings(ConfigSet::new().with_entry("datafusion.execution.batch_size", 128_u64));

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
}
