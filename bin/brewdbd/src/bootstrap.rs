//! Server bootstrap and dependency initialization.

use brewdb_frontend::pgwire::DirectPgwireService;

/// Top-level server bootstrap state.
#[derive(Clone, Debug, Default)]
pub struct ServerBootstrap {
    pub pgwire: DirectPgwireService,
}

impl ServerBootstrap {
    pub fn initialize() -> Self {
        Self::default()
    }
}
