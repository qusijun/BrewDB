//! Runtime-facing shared contracts.

use crate::common::config::ConfigSet;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryContext {
    pub query_id: Uuid,
    pub session_id: Uuid,
    pub user_name: String,
    pub database_name: Option<String>,
    pub catalog_name: Option<String>,
    pub settings: ConfigSet,
}

impl QueryContext {
    pub fn new(
        query_id: Uuid,
        session_id: Uuid,
        user_name: impl Into<String>,
        database_name: Option<String>,
        catalog_name: Option<String>,
        settings: ConfigSet,
    ) -> Self {
        Self {
            query_id,
            session_id,
            user_name: user_name.into(),
            database_name,
            catalog_name,
            settings,
        }
    }

    pub fn system(query_id: Uuid) -> Self {
        Self::new(
            query_id,
            Uuid::nil(),
            "system",
            None,
            None,
            ConfigSet::new(),
        )
    }

    pub fn with_settings(mut self, settings: ConfigSet) -> Self {
        self.settings = self.settings.merge_config_set(&settings);
        self
    }

    pub fn for_test(query_id: Uuid) -> Self {
        Self::system(query_id)
    }
}

#[cfg(test)]
mod tests {
    use super::QueryContext;
    use crate::common::config::ConfigSet;
    use uuid::Uuid;

    #[test]
    fn query_context_for_test_uses_system_session_values() {
        let context = QueryContext::for_test(Uuid::new_v4());
        assert_eq!(context.session_id, Uuid::nil());
        assert_eq!(context.user_name, "system");
        assert!(context.database_name.is_none());
        assert!(context.catalog_name.is_none());
        assert!(context.settings.is_empty());
    }

    #[test]
    fn query_context_carries_session_settings() {
        let session_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 8_u64);
        let context = QueryContext::new(
            Uuid::new_v4(),
            Uuid::nil(),
            "system",
            None,
            None,
            session_settings.clone(),
        );

        assert_eq!(context.settings, session_settings);
    }

    #[test]
    fn query_context_settings_override_session_settings() {
        let session_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 8_u64);
        let query_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 4_u64);
        let context = QueryContext::new(
            Uuid::new_v4(),
            Uuid::nil(),
            "system",
            None,
            None,
            session_settings,
        )
        .with_settings(query_settings);

        assert_eq!(
            context
                .settings
                .get_u64("brewdb.execution.max_threads")
                .unwrap(),
            Some(4)
        );
    }
}
