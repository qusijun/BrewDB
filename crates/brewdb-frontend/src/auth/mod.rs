//! Frontend authentication entry shell.

use crate::errors::FrontendError;

/// Authentication request attached to a frontend session bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthRequest {
    pub user_name: Option<String>,
    pub database_name: Option<String>,
}

/// Result of frontend authentication handling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub user_name: Option<String>,
    pub database_name: Option<String>,
    pub authenticated: bool,
}

/// Authentication boundary for frontend session startup.
pub trait Authenticator {
    fn authenticate(&self, request: AuthRequest) -> Result<AuthContext, FrontendError>;
}

/// Phase 1 permissive authenticator shell.
#[derive(Clone, Debug, Default)]
pub struct AllowAllAuthenticator;

impl Authenticator for AllowAllAuthenticator {
    fn authenticate(&self, request: AuthRequest) -> Result<AuthContext, FrontendError> {
        Ok(AuthContext {
            user_name: request.user_name,
            database_name: request.database_name,
            authenticated: true,
        })
    }
}
