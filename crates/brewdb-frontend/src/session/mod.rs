//! Frontend session shell.

use brewdb_core::common::RequestContext;
use brewdb_core::ids::SessionId;

use crate::auth::AuthContext;
use crate::errors::FrontendError;

/// Session bootstrap request entering the frontend layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenSession {
    pub request_context: RequestContext,
    pub auth_context: AuthContext,
}

/// Frontend-visible session state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendSession {
    pub session_id: SessionId,
    pub request_context: RequestContext,
    pub auth_context: AuthContext,
}

/// Session management boundary for SQL clients.
pub trait SessionService {
    fn open_session(&self, command: OpenSession) -> Result<FrontendSession, FrontendError>;
}

/// Phase 1 direct session service shell.
#[derive(Clone, Debug, Default)]
pub struct DirectSessionService;

impl SessionService for DirectSessionService {
    fn open_session(&self, command: OpenSession) -> Result<FrontendSession, FrontendError> {
        Ok(FrontendSession {
            session_id: SessionId::generate(),
            request_context: command.request_context,
            auth_context: command.auth_context,
        })
    }
}
