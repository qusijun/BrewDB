//! Protocol plugin contracts for the frontend boundary.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::errors::FrontendError;
use crate::result::FrontendResponse;
use crate::result::ResultField;
use crate::session::{ClientDefaults, FrontendService, SqlRequest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontendProtocolRequest {
    Startup {
        user: String,
        database: Option<String>,
    },
    Query {
        sql: String,
    },
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FrontendProtocolResponse {
    AuthenticationOk,
    ReadyForQuery,
    CommandComplete {
        tag: String,
    },
    RowDescription {
        fields: Vec<ResultField>,
    },
    NoticeResponse {
        severity: &'static str,
        message: String,
    },
}

pub trait FrontendProtocolPlugin: Send + Sync {
    fn protocol_name(&self) -> &'static str;

    fn decode_request(&self, payload: &[u8]) -> Result<FrontendProtocolRequest, FrontendError>;

    fn encode_response(&self, response: &FrontendResponse) -> Vec<FrontendProtocolResponse>;

    fn serve_connection(
        &self,
        stream: TcpStream,
        frontend: FrontendService,
        defaults: ClientDefaults,
        handler: Arc<dyn SqlRequestHandler>,
    ) -> Result<(), FrontendError>;
}

pub struct SqlExecutionResult {
    pub response: FrontendResponse,
    pub batches: Vec<RecordBatch>,
}

pub trait SqlRequestHandler: Send + Sync {
    fn execute(&self, request: &SqlRequest) -> Result<SqlExecutionResult, FrontendError>;
}

#[derive(Clone, Default)]
pub struct ProtocolRegistry {
    plugins: BTreeMap<&'static str, Arc<dyn FrontendProtocolPlugin>>,
}

impl ProtocolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtin_plugins() -> Self {
        let mut registry = Self::new();
        registry.register(Arc::new(crate::pgwire::PgWireCodec));
        registry
    }

    pub fn register(&mut self, plugin: Arc<dyn FrontendProtocolPlugin>) {
        self.plugins.insert(plugin.protocol_name(), plugin);
    }

    pub fn plugin(&self, protocol_name: &str) -> Option<Arc<dyn FrontendProtocolPlugin>> {
        self.plugins.get(protocol_name).cloned()
    }

    pub fn contains(&self, protocol_name: &str) -> bool {
        self.plugins.contains_key(protocol_name)
    }

    pub fn protocol_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.plugins.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::{FrontendProtocolRequest, ProtocolRegistry};
    use crate::pgwire::PgWireCodec;

    #[test]
    fn builtin_registry_exposes_pgwire_plugin() {
        let registry = ProtocolRegistry::with_builtin_plugins();
        let plugin = registry.plugin("pgwire").unwrap();

        assert_eq!(plugin.protocol_name(), "pgwire");
        assert_eq!(
            plugin.decode_request(b"select 1").unwrap(),
            FrontendProtocolRequest::Query {
                sql: "select 1".to_owned()
            }
        );
    }

    #[test]
    fn registry_can_replace_a_protocol_plugin() {
        let mut registry = ProtocolRegistry::new();
        registry.register(std::sync::Arc::new(PgWireCodec));
        registry.register(std::sync::Arc::new(PgWireCodec));

        assert_eq!(
            registry.protocol_names().collect::<Vec<_>>(),
            vec!["pgwire"]
        );
    }
}
