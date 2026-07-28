//! Shared logger initialization and structured log emission helpers.

use std::error::Error;

use tracing::{Level, event};
use tracing_subscriber::EnvFilter;

use crate::diagnostics::{LogEvent, LogLevel};

/// Logger initialization config shared by BrewDB binaries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggerConfig {
    pub service_name: &'static str,
    pub default_filter: &'static str,
}

impl LoggerConfig {
    pub const DATAFUSION_LOG_FILTER: &'static str = "datafusion=info,datafusion_catalog=info,datafusion_datasource=info,datafusion_execution=info,datafusion_expr=info,datafusion_functions=info,datafusion_functions_aggregate=info,datafusion_functions_nested=info,datafusion_functions_table=info,datafusion_functions_window=info,datafusion_optimizer=info,datafusion_physical_expr=info,datafusion_physical_optimizer=info,datafusion_physical_plan=info,datafusion_sql=info";

    pub const fn new(service_name: &'static str, default_filter: &'static str) -> Self {
        Self {
            service_name,
            default_filter,
        }
    }

    pub fn with_datafusion_targets(self) -> Self {
        Self {
            service_name: self.service_name,
            default_filter: if self.default_filter == "info,brewdb=debug" {
                "info,brewdb=debug,datafusion=info,datafusion_catalog=info,datafusion_datasource=info,datafusion_execution=info,datafusion_expr=info,datafusion_functions=info,datafusion_functions_aggregate=info,datafusion_functions_nested=info,datafusion_functions_table=info,datafusion_functions_window=info,datafusion_optimizer=info,datafusion_physical_expr=info,datafusion_physical_optimizer=info,datafusion_physical_plan=info,datafusion_sql=info"
            } else {
                self.default_filter
            },
        }
    }
}

pub type LoggerInitError = Box<dyn Error + Send + Sync>;

/// Install a process-global tracing subscriber for BrewDB binaries.
pub fn init_logger(config: LoggerConfig) -> Result<(), LoggerInitError> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config.default_filter))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_names(true)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .try_init()?;

    event!(
        target: "brewdb.bootstrap",
        Level::INFO,
        service = config.service_name,
        event_name = "logger.initialized",
        "logger initialized"
    );

    Ok(())
}

/// Emit one shared structured log event through the tracing backend.
pub fn emit_log_event(log_event: &LogEvent) {
    let error_code = log_event
        .error_code
        .map(|code| code.as_str())
        .unwrap_or("-");
    let request_id = log_event
        .context
        .request_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let session_id = log_event
        .context
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let job_id = log_event
        .context
        .job_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let stage_id = log_event
        .context
        .stage_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let task_id = log_event
        .context
        .task_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let txn_id = log_event
        .context
        .txn_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let fields = log_event
        .fields
        .iter()
        .map(|field| format!("{}={}", field.key, field.value))
        .collect::<Vec<_>>()
        .join(", ");

    match log_event.level {
        LogLevel::Trace => event!(
            target: "brewdb.event",
            Level::TRACE,
            event_target = log_event.target,
            event_name = log_event.event_name,
            error_code,
            request_id,
            session_id,
            job_id,
            stage_id,
            task_id,
            txn_id,
            fields = fields.as_str(),
            "{}",
            log_event.message
        ),
        LogLevel::Debug => event!(
            target: "brewdb.event",
            Level::DEBUG,
            event_target = log_event.target,
            event_name = log_event.event_name,
            error_code,
            request_id,
            session_id,
            job_id,
            stage_id,
            task_id,
            txn_id,
            fields = fields.as_str(),
            "{}",
            log_event.message
        ),
        LogLevel::Info => event!(
            target: "brewdb.event",
            Level::INFO,
            event_target = log_event.target,
            event_name = log_event.event_name,
            error_code,
            request_id,
            session_id,
            job_id,
            stage_id,
            task_id,
            txn_id,
            fields = fields.as_str(),
            "{}",
            log_event.message
        ),
        LogLevel::Warn => event!(
            target: "brewdb.event",
            Level::WARN,
            event_target = log_event.target,
            event_name = log_event.event_name,
            error_code,
            request_id,
            session_id,
            job_id,
            stage_id,
            task_id,
            txn_id,
            fields = fields.as_str(),
            "{}",
            log_event.message
        ),
        LogLevel::Error => event!(
            target: "brewdb.event",
            Level::ERROR,
            event_target = log_event.target,
            event_name = log_event.event_name,
            error_code,
            request_id,
            session_id,
            job_id,
            stage_id,
            task_id,
            txn_id,
            fields = fields.as_str(),
            "{}",
            log_event.message
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::diagnostics::{DiagnosticContext, ErrorCode, LogEvent, LogLevel};
    use crate::ids::JobId;

    use super::LoggerConfig;

    #[test]
    fn logger_config_keeps_service_defaults() {
        let config = LoggerConfig::new("brewdbd", "info,brewdb=debug");

        assert_eq!(config.service_name, "brewdbd");
        assert_eq!(config.default_filter, "info,brewdb=debug");
    }

    #[test]
    fn logger_config_can_enable_datafusion_targets() {
        let config = LoggerConfig::new("brewdbd", "info,brewdb=debug").with_datafusion_targets();

        assert!(config.default_filter.contains("datafusion=info"));
        assert!(
            config
                .default_filter
                .contains("datafusion_physical_plan=info")
        );
        assert!(config.default_filter.contains("datafusion_sql=info"));
    }

    #[test]
    fn log_event_can_be_prepared_for_runtime_emission() {
        let event = LogEvent::new(
            LogLevel::Info,
            "brewdb.runtime",
            "job.admitted",
            "job admitted into runtime ownership",
        )
        .with_error_code(ErrorCode::RuntimeMissingField)
        .with_context(
            DiagnosticContext::default()
                .with_job_id(JobId::parse_str("550e8400-e29b-41d4-a716-446655441420").unwrap()),
        );

        assert_eq!(event.target, "brewdb.runtime");
        assert!(event.context.job_id.is_some());
    }
}
