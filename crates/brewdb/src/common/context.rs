//! Runtime-facing shared contracts.

use crate::common::config::ConfigSet;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionContext {
    pub session_id: Uuid,
    pub user_name: String,
    pub database_name: Option<String>,
    pub catalog_name: Option<String>,
    pub settings: ConfigSet,
}

impl SessionContext {
    pub fn system() -> Self {
        Self {
            session_id: Uuid::nil(),
            user_name: "system".to_owned(),
            database_name: None,
            catalog_name: None,
            settings: ConfigSet::new(),
        }
    }

    pub fn with_settings(mut self, settings: ConfigSet) -> Self {
        self.settings = settings;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryContext {
    pub query_id: Uuid,
    pub session: SessionContext,
    pub settings: ConfigSet,
}

impl QueryContext {
    pub fn new(query_id: Uuid, session: SessionContext) -> Self {
        let settings = session.settings.clone();
        Self {
            query_id,
            session,
            settings,
        }
    }

    pub fn with_settings(mut self, settings: ConfigSet) -> Self {
        self.settings = self.session.settings.merge_config_set(&settings);
        self
    }

    pub fn for_test(query_id: Uuid) -> Self {
        Self::new(query_id, SessionContext::system())
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryContext, SessionContext};
    use crate::common::config::ConfigSet;
    use uuid::Uuid;

    #[test]
    fn session_context_system_initializes_empty_settings() {
        let session = SessionContext::system();
        assert!(session.settings.is_empty());
    }

    #[test]
    fn query_context_for_test_uses_system_session_context() {
        let context = QueryContext::for_test(Uuid::new_v4());
        assert_eq!(context.session, SessionContext::system());
    }

    #[test]
    fn session_context_with_settings_overrides_default_settings() {
        let settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 8_u64);
        let session = SessionContext::system().with_settings(settings.clone());
        assert_eq!(session.settings, settings);
    }

    #[test]
    fn query_context_inherits_session_settings() {
        let session_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 8_u64);
        let context = QueryContext::new(
            Uuid::new_v4(),
            SessionContext::system().with_settings(session_settings.clone()),
        );

        assert_eq!(context.settings, session_settings);
    }

    #[test]
    fn query_context_settings_override_session_settings() {
        let session_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 8_u64);
        let query_settings = ConfigSet::new().with_entry("brewdb.execution.max_threads", 4_u64);
        let context = QueryContext::new(
            Uuid::new_v4(),
            SessionContext::system().with_settings(session_settings),
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
