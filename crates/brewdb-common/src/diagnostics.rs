//! Shared diagnostics primitives.

use uuid::Uuid;

/// Stable error-code namespace used across BrewDB crates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ErrorCode(&'static str);

impl ErrorCode {
    pub const INTERNAL: Self = Self::new("BREWDB_COMMON_INTERNAL");
    pub const NOT_FOUND: Self = Self::new("BREWDB_COMMON_NOT_FOUND");
    pub const ALREADY_EXISTS: Self = Self::new("BREWDB_COMMON_ALREADY_EXISTS");
    pub const NOT_IMPLEMENTED: Self = Self::new("BREWDB_COMMON_NOT_IMPLEMENTED");
    pub const INVALID_CONFIGURATION: Self = Self::new("BREWDB_COMMON_INVALID_CONFIGURATION");
    pub const LOGGING_INITIALIZATION_FAILED: Self = Self::new("BREWDB_COMMON_LOGGING_INIT_FAILED");

    pub const fn new(code: &'static str) -> Self {
        Self(code)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Optional structured diagnostics contract for crate-local errors.
pub trait DiagnosticError {
    fn error_code(&self) -> ErrorCode;

    fn log_target(&self) -> &'static str;

    fn diagnostic_context(&self, event_name: &'static str) -> DiagnosticContext {
        DiagnosticContext::new(self.log_target(), event_name).with_error_code(self.error_code())
    }
}

/// Structured diagnostics context attached to emitted events and surfaced errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiagnosticContext {
    pub target: &'static str,
    pub event_name: &'static str,
    pub error_code: Option<ErrorCode>,
    pub error_variant: Option<&'static str>,
    pub job_id: Option<String>,
}

impl DiagnosticContext {
    pub fn new(target: &'static str, event_name: &'static str) -> Self {
        Self {
            target,
            event_name,
            error_code: None,
            error_variant: None,
            job_id: None,
        }
    }

    pub fn with_error_code(mut self, error_code: ErrorCode) -> Self {
        self.error_code = Some(error_code);
        self
    }

    pub fn with_job_id(mut self, job_id: Uuid) -> Self {
        self.job_id = Some(job_id.to_string());
        self
    }

    pub fn with_error_variant(mut self, error_variant: &'static str) -> Self {
        self.error_variant = Some(error_variant);
        self
    }

    pub fn error_code_str(&self) -> Option<&'static str> {
        self.error_code.map(|code| code.as_str())
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{DiagnosticContext, DiagnosticError, ErrorCode};

    #[test]
    fn diagnostic_context_keeps_error_code_and_job_id() {
        let job_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440011").unwrap();
        let context = DiagnosticContext::new("brewdb.catalog", "catalog.lookup")
            .with_error_code(ErrorCode::INTERNAL)
            .with_error_variant("TableNotFound")
            .with_job_id(job_id);

        assert_eq!(context.target, "brewdb.catalog");
        assert_eq!(context.event_name, "catalog.lookup");
        assert_eq!(context.error_code, Some(ErrorCode::INTERNAL));
        assert_eq!(context.error_variant, Some("TableNotFound"));
        assert_eq!(
            context.job_id.as_deref(),
            Some("550e8400-e29b-41d4-a716-446655440011")
        );
    }

    #[test]
    fn diagnostic_error_builds_default_context() {
        struct TestError;

        impl DiagnosticError for TestError {
            fn error_code(&self) -> ErrorCode {
                ErrorCode::INTERNAL
            }

            fn log_target(&self) -> &'static str {
                "brewdb.test"
            }
        }

        let context = TestError.diagnostic_context("test.failure");

        assert_eq!(context.target, "brewdb.test");
        assert_eq!(context.event_name, "test.failure");
        assert_eq!(context.error_code_str(), Some("BREWDB_COMMON_INTERNAL"));
    }
}
