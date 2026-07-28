//! Server entrypoints and interface startup.

use brewdb_core::common::RequestContext;
use brewdb_frontend::pgwire::{PgwireQueryRequest, PgwireService};

use crate::bootstrap::ServerBootstrap;

/// Minimal server start summary used by the current framework shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerStartSummary {
    pub stage_count: usize,
}

pub fn start_server(bootstrap: &ServerBootstrap) -> Result<ServerStartSummary, String> {
    let bootstrap_result = bootstrap
        .pgwire
        .bootstrap_query(PgwireQueryRequest {
            sql: "select 1".to_owned(),
            request_context: RequestContext::new(),
            user_name: Some("brew".to_owned()),
            database_name: Some("default".to_owned()),
        })
        .map_err(|error| error.to_string())?;

    Ok(ServerStartSummary {
        stage_count: bootstrap_result.admission.stage_ids().len(),
    })
}
