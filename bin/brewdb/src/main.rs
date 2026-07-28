mod cli;
mod commands;
mod config;

use brewdb_core::logging::{LoggerConfig, init_logger};
use commands::{bootstrap_minimal_query, sample_query_request};

fn main() {
    init_logger(LoggerConfig::new("brewdb", "info,brewdb=debug").with_datafusion_targets())
        .expect("failed to initialize brewdb logger");

    match bootstrap_minimal_query(sample_query_request()) {
        Ok(bootstrap) => {
            tracing::info!(
                target: "brewdb.cli",
                event_name = "cli.query_bootstrap",
                job_id = %bootstrap.job_id,
                stage_count = bootstrap.admission.stage_ids().len(),
                "bootstrapped minimal query loop into runtime admission"
            );
        }
        Err(error) => {
            tracing::error!(
                target: "brewdb.cli",
                event_name = "cli.query_bootstrap_failed",
                error = %error,
                "failed to bootstrap minimal query loop"
            );
            std::process::exit(1);
        }
    }
}
