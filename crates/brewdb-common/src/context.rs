//! Runtime-facing shared contracts.

use crate::common::config::ConfigSet;
use std::fmt;
use tracing::Span;
use uuid::Uuid;

#[derive(Clone)]
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

    pub fn span(&self) -> Span {
        tracing::info_span!(
            "query",
            query_id = %self.query_id,
            session_id = %self.session_id,
            user = self.user_name.as_str(),
            database = self.database_name.as_deref().unwrap_or(""),
            catalog = self.catalog_name.as_deref().unwrap_or("")
        )
    }

    pub fn for_test(query_id: Uuid) -> Self {
        Self::system(query_id)
    }
}

impl fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryContext")
            .field("query_id", &self.query_id)
            .field("session_id", &self.session_id)
            .field("user_name", &self.user_name)
            .field("database_name", &self.database_name)
            .field("catalog_name", &self.catalog_name)
            .field("settings", &self.settings)
            .finish()
    }
}

impl PartialEq for QueryContext {
    fn eq(&self, other: &Self) -> bool {
        self.query_id == other.query_id
            && self.session_id == other.session_id
            && self.user_name == other.user_name
            && self.database_name == other.database_name
            && self.catalog_name == other.catalog_name
            && self.settings == other.settings
    }
}

impl Eq for QueryContext {}

#[cfg(test)]
mod tests {
    use super::QueryContext;
    use crate::common::config::ConfigSet;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use tracing::info;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

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

    #[test]
    fn query_context_span_exposes_query_identity_in_logs() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = {
            let buffer = buffer.clone();
            move || BufferWriter(buffer.clone())
        };
        let subscriber = tracing_subscriber::fmt()
            .compact()
            .with_writer(writer)
            .finish();

        let query_id = Uuid::new_v4();
        let context = QueryContext::new(
            query_id,
            Uuid::new_v4(),
            "brew",
            None,
            None,
            ConfigSet::new(),
        );

        tracing::subscriber::with_default(subscriber, || {
            let span = context.span();
            let _guard = span.enter();
            info!("query span test");
        });

        let output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(output.contains(&query_id.to_string()));
    }
}
