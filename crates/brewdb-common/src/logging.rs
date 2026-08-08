//! Process-wide logging bootstrap for BrewDB.

use tracing::{debug, error, info, trace, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::diagnostics::DiagnosticContext;
use crate::errors::CommonError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Compact,
    Pretty,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggingConfig {
    pub filter: String,
    pub format: LogFormat,
    pub include_target: bool,
    pub include_thread_names: bool,
    pub include_datafusion_targets: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter: default_filter(true),
            format: LogFormat::Compact,
            include_target: true,
            include_thread_names: true,
            include_datafusion_targets: true,
        }
    }
}

pub fn default_filter(include_datafusion_targets: bool) -> String {
    if include_datafusion_targets {
        "info,datafusion=info".to_owned()
    } else {
        "info".to_owned()
    }
}

pub fn init_logging(config: &LoggingConfig) -> Result<(), CommonError> {
    let filter = if config.filter.trim().is_empty() {
        default_filter(config.include_datafusion_targets)
    } else {
        config.filter.clone()
    };
    let env_filter =
        EnvFilter::try_new(filter).map_err(|error| CommonError::InvalidConfiguration {
            field: "logging.filter".to_owned(),
            reason: error.to_string(),
        })?;

    match config.format {
        LogFormat::Compact => tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .compact()
                    .with_target(config.include_target)
                    .with_thread_names(config.include_thread_names),
            )
            .try_init()
            .map_err(|error| CommonError::LoggingInitializationFailed {
                reason: error.to_string(),
            }),
        LogFormat::Pretty => tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .pretty()
                    .with_target(config.include_target)
                    .with_thread_names(config.include_thread_names),
            )
            .try_init()
            .map_err(|error| CommonError::LoggingInitializationFailed {
                reason: error.to_string(),
            }),
        LogFormat::Json => tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .json()
                    .with_target(config.include_target)
                    .with_thread_names(config.include_thread_names),
            )
            .try_init()
            .map_err(|error| CommonError::LoggingInitializationFailed {
                reason: error.to_string(),
            }),
    }
}

pub fn emit_event(level: LogLevel, context: &DiagnosticContext, message: &str) {
    let error_code = context.error_code_str().unwrap_or("");
    let error_variant = context.error_variant.unwrap_or("");
    let job_id = context.job_id.as_deref().unwrap_or("");

    match level {
        LogLevel::Trace => trace!(
            target: "brewdb.event",
            event_target = context.target,
            event_name = context.event_name,
            error_code = error_code,
            error_variant = error_variant,
            job_id = job_id,
            "{message}"
        ),
        LogLevel::Debug => debug!(
            target: "brewdb.event",
            event_target = context.target,
            event_name = context.event_name,
            error_code = error_code,
            error_variant = error_variant,
            job_id = job_id,
            "{message}"
        ),
        LogLevel::Info => info!(
            target: "brewdb.event",
            event_target = context.target,
            event_name = context.event_name,
            error_code = error_code,
            error_variant = error_variant,
            job_id = job_id,
            "{message}"
        ),
        LogLevel::Warn => warn!(
            target: "brewdb.event",
            event_target = context.target,
            event_name = context.event_name,
            error_code = error_code,
            error_variant = error_variant,
            job_id = job_id,
            "{message}"
        ),
        LogLevel::Error => error!(
            target: "brewdb.event",
            event_target = context.target,
            event_name = context.event_name,
            error_code = error_code,
            error_variant = error_variant,
            job_id = job_id,
            "{message}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::diagnostics::{DiagnosticContext, ErrorCode};

    use super::{LogFormat, LogLevel, LoggingConfig, default_filter, emit_event};

    #[test]
    fn logging_config_defaults_match_process_bootstrap_expectations() {
        let config = LoggingConfig::default();

        assert_eq!(config.filter, "info,datafusion=info");
        assert_eq!(config.format, LogFormat::Compact);
        assert!(config.include_target);
        assert!(config.include_thread_names);
        assert!(config.include_datafusion_targets);
    }

    #[test]
    fn default_filter_can_drop_datafusion_targets() {
        assert_eq!(default_filter(false), "info");
        assert_eq!(default_filter(true), "info,datafusion=info");
    }

    #[test]
    fn emit_event_accepts_structured_diagnostic_context() {
        let context = DiagnosticContext::new("brewdb.test", "logger.test")
            .with_error_code(ErrorCode::INTERNAL);

        emit_event(LogLevel::Info, &context, "logger helper smoke test");
    }
}
