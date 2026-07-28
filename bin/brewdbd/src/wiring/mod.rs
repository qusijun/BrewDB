//! Runtime wiring and service assembly.

use crate::bootstrap::ServerBootstrap;

pub fn assemble_server() -> ServerBootstrap {
    ServerBootstrap::initialize()
}
