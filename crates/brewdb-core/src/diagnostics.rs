//! Shared diagnostics primitives for logging and stable error-code reporting.

use crate::common::RequestContext;
use crate::ids::{JobId, RequestId, SessionId, StageId, TaskId, TxnId};

/// Stable crate/domain ownership for diagnostics and error codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorDomain {
    Core,
    Catalog,
    Frontend,
    Sql,
    Execution,
    Storage,
    Runtime,
}

/// Stable workspace-wide error code catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    CoreInvalidStateTransition,
    CoreInvalidIdentifier,
    CatalogMissingField,
    CatalogNotFound,
    CatalogUnsupported,
    FrontendMissingField,
    FrontendUnsupported,
    SqlMissingField,
    SqlUnsupported,
    ExecutionMissingField,
    ExecutionInvalidPlan,
    StorageMissingField,
    StorageUnsupported,
    RuntimeMissingField,
    RuntimeStateConflict,
    RuntimeNotFound,
}

impl ErrorCode {
    pub const fn domain(self) -> ErrorDomain {
        match self {
            Self::CoreInvalidStateTransition | Self::CoreInvalidIdentifier => ErrorDomain::Core,
            Self::CatalogMissingField | Self::CatalogNotFound | Self::CatalogUnsupported => {
                ErrorDomain::Catalog
            }
            Self::FrontendMissingField | Self::FrontendUnsupported => ErrorDomain::Frontend,
            Self::SqlMissingField | Self::SqlUnsupported => ErrorDomain::Sql,
            Self::ExecutionMissingField | Self::ExecutionInvalidPlan => ErrorDomain::Execution,
            Self::StorageMissingField | Self::StorageUnsupported => ErrorDomain::Storage,
            Self::RuntimeMissingField | Self::RuntimeStateConflict | Self::RuntimeNotFound => {
                ErrorDomain::Runtime
            }
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CoreInvalidStateTransition => "CORE-STATE-001",
            Self::CoreInvalidIdentifier => "CORE-ID-001",
            Self::CatalogMissingField => "CATALOG-INPUT-001",
            Self::CatalogNotFound => "CATALOG-LOOKUP-404",
            Self::CatalogUnsupported => "CATALOG-UNSUPPORTED-001",
            Self::FrontendMissingField => "FRONTEND-INPUT-001",
            Self::FrontendUnsupported => "FRONTEND-UNSUPPORTED-001",
            Self::SqlMissingField => "SQL-INPUT-001",
            Self::SqlUnsupported => "SQL-UNSUPPORTED-001",
            Self::ExecutionMissingField => "EXEC-INPUT-001",
            Self::ExecutionInvalidPlan => "EXEC-PLAN-001",
            Self::StorageMissingField => "STORAGE-INPUT-001",
            Self::StorageUnsupported => "STORAGE-UNSUPPORTED-001",
            Self::RuntimeMissingField => "RUNTIME-INPUT-001",
            Self::RuntimeStateConflict => "RUNTIME-STATE-001",
            Self::RuntimeNotFound => "RUNTIME-LOOKUP-404",
        }
    }
}

/// Common log levels used by shared diagnostics events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Request and runtime identity carried alongside structured log events.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DiagnosticContext {
    pub request_id: Option<RequestId>,
    pub session_id: Option<SessionId>,
    pub job_id: Option<JobId>,
    pub stage_id: Option<StageId>,
    pub task_id: Option<TaskId>,
    pub txn_id: Option<TxnId>,
}

impl DiagnosticContext {
    pub fn from_request_context(request_context: &RequestContext) -> Self {
        Self {
            request_id: request_context.request_id.clone(),
            session_id: request_context.session_id.clone(),
            job_id: None,
            stage_id: None,
            task_id: None,
            txn_id: None,
        }
    }

    pub fn with_job_id(mut self, job_id: JobId) -> Self {
        self.job_id = Some(job_id);
        self
    }

    pub fn with_stage_id(mut self, stage_id: StageId) -> Self {
        self.stage_id = Some(stage_id);
        self
    }

    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn with_txn_id(mut self, txn_id: TxnId) -> Self {
        self.txn_id = Some(txn_id);
        self
    }
}

/// One structured key/value field on a log event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogField {
    pub key: &'static str,
    pub value: String,
}

/// Shared structured log event shell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEvent {
    pub level: LogLevel,
    pub target: &'static str,
    pub event_name: &'static str,
    pub message: String,
    pub error_code: Option<ErrorCode>,
    pub context: DiagnosticContext,
    pub fields: Vec<LogField>,
}

impl LogEvent {
    pub fn new(
        level: LogLevel,
        target: &'static str,
        event_name: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            level,
            target,
            event_name,
            message: message.into(),
            error_code: None,
            context: DiagnosticContext::default(),
            fields: Vec::new(),
        }
    }

    pub fn with_error_code(mut self, error_code: ErrorCode) -> Self {
        self.error_code = Some(error_code);
        self
    }

    pub fn with_context(mut self, context: DiagnosticContext) -> Self {
        self.context = context;
        self
    }

    pub fn with_field(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.fields.push(LogField {
            key,
            value: value.into(),
        });
        self
    }
}

/// Shared contract for crate-local errors that expose stable codes and log targets.
pub trait DiagnosticError {
    fn error_code(&self) -> ErrorCode;

    fn log_target(&self) -> &'static str;

    fn to_log_event(&self, event_name: &'static str, context: DiagnosticContext) -> LogEvent
    where
        Self: std::fmt::Display,
    {
        LogEvent::new(
            LogLevel::Error,
            self.log_target(),
            event_name,
            self.to_string(),
        )
        .with_error_code(self.error_code())
        .with_context(context)
    }
}

#[cfg(test)]
mod tests {
    use crate::common::RequestContext;
    use crate::ids::{JobId, RequestId};

    use super::{DiagnosticContext, ErrorCode, ErrorDomain, LogEvent, LogLevel};

    #[test]
    fn error_code_exposes_stable_string_and_domain() {
        assert_eq!(
            ErrorCode::RuntimeStateConflict.as_str(),
            "RUNTIME-STATE-001"
        );
        assert_eq!(
            ErrorCode::RuntimeStateConflict.domain(),
            ErrorDomain::Runtime
        );
    }

    #[test]
    fn diagnostic_context_can_be_seeded_from_request_context() {
        let request_context = RequestContext::new()
            .with_request_id(RequestId::parse_str("550e8400-e29b-41d4-a716-446655441400").unwrap());
        let context = DiagnosticContext::from_request_context(&request_context)
            .with_job_id(JobId::parse_str("550e8400-e29b-41d4-a716-446655441401").unwrap());

        assert!(context.request_id.is_some());
        assert!(context.job_id.is_some());
    }

    #[test]
    fn log_event_keeps_code_context_and_fields() {
        let event = LogEvent::new(
            LogLevel::Warn,
            "brewdb.runtime",
            "dispatch.backpressure",
            "worker slots exhausted",
        )
        .with_error_code(ErrorCode::RuntimeStateConflict)
        .with_field("worker_id", "worker-a");

        assert_eq!(event.error_code, Some(ErrorCode::RuntimeStateConflict));
        assert_eq!(event.fields.len(), 1);
        assert_eq!(event.fields[0].key, "worker_id");
    }
}
