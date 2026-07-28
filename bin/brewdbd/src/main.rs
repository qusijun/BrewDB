mod bootstrap;
mod config;
mod server;
mod wiring;

use brewdb_core::logging::{LoggerConfig, init_logger};
use server::start_server;
use wiring::assemble_server;

fn main() {
    init_logger(LoggerConfig::new("brewdbd", "info,brewdb=debug").with_datafusion_targets())
        .expect("failed to initialize brewdbd logger");

    let bootstrap = assemble_server();
    match start_server(&bootstrap) {
        Ok(summary) => tracing::info!(
            target: "brewdb.server",
            event_name = "server.pgwire_bootstrap",
            stage_count = summary.stage_count,
            "bootstrapped brewdbd pgwire ingress shell"
        ),
        Err(error) => {
            tracing::error!(
                target: "brewdb.server",
                event_name = "server.pgwire_bootstrap_failed",
                error = %error,
                "failed to bootstrap brewdbd pgwire ingress shell"
            );
            std::process::exit(1);
        }
    }
}
