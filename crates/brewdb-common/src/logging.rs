//! Process-wide logging bootstrap for BrewDB.

use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use tracing::{debug, error, info, trace, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::EnvFilter;

use crate::common::config::{
    ConfigDefinition, ConfigScope, ConfigSet, ConfigValueKind, ConfigView,
};
use crate::common::diagnostics::DiagnosticContext;
use crate::common::errors::CommonError;

static LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

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

impl LogLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollingPolicy {
    Never,
    Minutely,
    Hourly,
    Daily,
}

impl RollingPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Minutely => "minutely",
            Self::Hourly => "hourly",
            Self::Daily => "daily",
        }
    }

    const fn as_rotation(self) -> Rotation {
        match self {
            Self::Never => Rotation::NEVER,
            Self::Minutely => Rotation::MINUTELY,
            Self::Hourly => Rotation::HOURLY,
            Self::Daily => Rotation::DAILY,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub filter: String,
    pub path: Option<String>,
    pub rolling_policy: RollingPolicy,
    pub format: LogFormat,
    pub include_target: bool,
    pub include_thread_names: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            filter: default_filter(LogLevel::Info),
            path: None,
            rolling_policy: RollingPolicy::Never,
            format: LogFormat::Compact,
            include_target: true,
            include_thread_names: true,
        }
    }
}

impl LoggingConfig {
    pub fn from_config_set(config: &ConfigSet) -> Result<Self, CommonError> {
        <Self as ConfigView>::from_config_set(config)
    }
}

impl ConfigView for LoggingConfig {
    fn config_definitions() -> Vec<ConfigDefinition> {
        crate::config_definitions!(
            (
                "brewdb.logging.level",
                ConfigValueKind::String,
                "info",
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.filter",
                ConfigValueKind::String,
                "",
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.path",
                ConfigValueKind::String,
                "",
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.rolling_policy",
                ConfigValueKind::String,
                "never",
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.format",
                ConfigValueKind::String,
                "compact",
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.include_target",
                ConfigValueKind::Bool,
                true,
                [ConfigScope::System]
            ),
            (
                "brewdb.logging.include_thread_names",
                ConfigValueKind::Bool,
                true,
                [ConfigScope::System]
            ),
        )
    }

    fn from_config_set(config: &ConfigSet) -> Result<Self, CommonError> {
        let level = parse_log_level(config.require_string("brewdb.logging.level")?)?;
        let configured_filter = config.require_string("brewdb.logging.filter")?.trim();

        Ok(Self {
            level,
            filter: if configured_filter.is_empty() {
                default_filter(level)
            } else {
                configured_filter.to_owned()
            },
            path: parse_optional_path(config.require_string("brewdb.logging.path")?),
            rolling_policy: parse_rolling_policy(
                config.require_string("brewdb.logging.rolling_policy")?,
            )?,
            format: parse_log_format(config.require_string("brewdb.logging.format")?)?,
            include_target: config.require_bool("brewdb.logging.include_target")?,
            include_thread_names: config.require_bool("brewdb.logging.include_thread_names")?,
        })
    }
}

crate::common::config::inventory::submit! {
    crate::common::config::ConfigDefinitionSetRegistration {
        collect: <LoggingConfig as ConfigView>::config_definitions,
    }
}

pub fn default_filter(level: LogLevel) -> String {
    level.as_str().to_owned()
}

pub fn init_logging(config: &LoggingConfig) -> Result<(), CommonError> {
    let filter = if config.filter.trim().is_empty() {
        default_filter(config.level)
    } else {
        config.filter.clone()
    };
    let env_filter =
        EnvFilter::try_new(filter).map_err(|error| CommonError::InvalidConfiguration {
            field: "brewdb.logging.filter".to_owned(),
            reason: error.to_string(),
        })?;

    if let Some(path) = &config.path {
        let appender = rolling_appender(Path::new(path), config.rolling_policy)?;
        let (writer, guard) = tracing_appender::non_blocking(appender);
        let result = match config.format {
            LogFormat::Compact => tracing_subscriber::fmt()
                .compact()
                .with_env_filter(env_filter)
                .with_writer(writer)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
            LogFormat::Pretty => tracing_subscriber::fmt()
                .pretty()
                .with_env_filter(env_filter)
                .with_writer(writer)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
            LogFormat::Json => tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_writer(writer)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
        };

        result.map_err(|error| CommonError::LoggingInitializationFailed {
            reason: error.to_string(),
        })?;
        let _ = LOG_GUARD.set(guard);
        Ok(())
    } else {
        match config.format {
            LogFormat::Compact => tracing_subscriber::fmt()
                .compact()
                .with_env_filter(env_filter)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
            LogFormat::Pretty => tracing_subscriber::fmt()
                .pretty()
                .with_env_filter(env_filter)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
            LogFormat::Json => tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_target(config.include_target)
                .with_thread_names(config.include_thread_names)
                .try_init(),
        }
        .map_err(|error| CommonError::LoggingInitializationFailed {
            reason: error.to_string(),
        })
    }
}

fn rolling_appender(
    log_path: &Path,
    rolling_policy: RollingPolicy,
) -> Result<RollingFileAppender, CommonError> {
    let directory = log_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory).map_err(|error| CommonError::InvalidConfiguration {
        field: "brewdb.logging.path".to_owned(),
        reason: format!(
            "failed to create log directory `{}`: {error}",
            directory.display()
        ),
    })?;

    let filename = log_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| CommonError::InvalidConfiguration {
            field: "brewdb.logging.path".to_owned(),
            reason: format!(
                "log path `{}` must include a utf-8 file name",
                log_path.display()
            ),
        })?;

    RollingFileAppender::builder()
        .rotation(rolling_policy.as_rotation())
        .filename_prefix(filename)
        .build(directory)
        .map_err(|error| CommonError::InvalidConfiguration {
            field: "brewdb.logging.path".to_owned(),
            reason: error.to_string(),
        })
}

fn parse_optional_path(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn parse_log_level(value: &str) -> Result<LogLevel, CommonError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(LogLevel::Trace),
        "debug" => Ok(LogLevel::Debug),
        "info" => Ok(LogLevel::Info),
        "warn" | "warning" => Ok(LogLevel::Warn),
        "error" => Ok(LogLevel::Error),
        other => Err(CommonError::InvalidConfiguration {
            field: "brewdb.logging.level".to_owned(),
            reason: format!("unsupported log level `{other}`"),
        }),
    }
}

fn parse_log_format(value: &str) -> Result<LogFormat, CommonError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "compact" => Ok(LogFormat::Compact),
        "pretty" => Ok(LogFormat::Pretty),
        "json" => Ok(LogFormat::Json),
        other => Err(CommonError::InvalidConfiguration {
            field: "brewdb.logging.format".to_owned(),
            reason: format!("unsupported log format `{other}`"),
        }),
    }
}

fn parse_rolling_policy(value: &str) -> Result<RollingPolicy, CommonError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "never" => Ok(RollingPolicy::Never),
        "minutely" | "minute" => Ok(RollingPolicy::Minutely),
        "hourly" | "hour" => Ok(RollingPolicy::Hourly),
        "daily" | "day" => Ok(RollingPolicy::Daily),
        other => Err(CommonError::InvalidConfiguration {
            field: "brewdb.logging.rolling_policy".to_owned(),
            reason: format!("unsupported rolling policy `{other}`"),
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
    use crate::common::config::{ConfigRegistry, SystemConfigLoader};
    use crate::common::diagnostics::{DiagnosticContext, ErrorCode};

    use super::{LogFormat, LogLevel, LoggingConfig, RollingPolicy, default_filter, emit_event};

    #[test]
    fn logging_config_defaults_match_process_bootstrap_expectations() {
        let config = LoggingConfig::default();

        assert_eq!(config.filter, "info");
        assert_eq!(config.format, LogFormat::Compact);
        assert_eq!(config.level, LogLevel::Info);
        assert_eq!(config.path, None);
        assert_eq!(config.rolling_policy, RollingPolicy::Never);
        assert!(config.include_target);
        assert!(config.include_thread_names);
    }

    #[test]
    fn default_filter_uses_process_level_for_all_targets() {
        assert_eq!(default_filter(LogLevel::Info), "info");
        assert_eq!(default_filter(LogLevel::Debug), "debug");
    }

    #[test]
    fn logging_config_loads_target_filter_from_system_config() {
        let registry = ConfigRegistry::for_view::<LoggingConfig>().unwrap();
        let loader = SystemConfigLoader::new(registry);
        let config_set = loader
            .load_toml_str(
                r#"
                brewdb.logging.level = "debug"
                brewdb.logging.filter = "info,datafusion=warn,paimon=debug"
                brewdb.logging.path = "/tmp/brewdbd.log"
                brewdb.logging.rolling_policy = "daily"
                brewdb.logging.format = "json"
                brewdb.logging.include_target = false
                brewdb.logging.include_thread_names = false
                "#,
            )
            .unwrap();

        let config = LoggingConfig::from_config_set(&config_set).unwrap();

        assert_eq!(config.level, LogLevel::Debug);
        assert_eq!(config.path.as_deref(), Some("/tmp/brewdbd.log"));
        assert_eq!(config.rolling_policy, RollingPolicy::Daily);
        assert_eq!(config.format, LogFormat::Json);
        assert!(!config.include_target);
        assert!(!config.include_thread_names);
        assert_eq!(config.filter, "info,datafusion=warn,paimon=debug");
    }

    #[test]
    fn emit_event_accepts_structured_diagnostic_context() {
        let context = DiagnosticContext::new("brewdb.test", "logger.test")
            .with_error_code(ErrorCode::INTERNAL);

        emit_event(LogLevel::Info, &context, "logger helper smoke test");
    }
}
