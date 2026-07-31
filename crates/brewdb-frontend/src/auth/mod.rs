//! Authentication entry contracts for frontend sessions.

use crate::errors::FrontendError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthMethod {
    Trust,
    Password,
    Token,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub user_name: String,
    pub database_name: Option<String>,
    pub method: AuthMethod,
    pub secret: Option<String>,
}

impl AuthContext {
    pub fn new(user_name: impl Into<String>, method: AuthMethod) -> Self {
        Self {
            user_name: user_name.into(),
            database_name: None,
            method,
            secret: None,
        }
    }

    pub fn with_database(mut self, database_name: impl Into<String>) -> Self {
        self.database_name = Some(database_name.into());
        self
    }

    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(secret.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthDecision {
    pub authenticated: bool,
    pub effective_user: String,
    pub database_name: Option<String>,
}

impl AuthDecision {
    pub fn allow(context: &AuthContext) -> Self {
        Self {
            authenticated: true,
            effective_user: context.user_name.clone(),
            database_name: context.database_name.clone(),
        }
    }
}

pub trait Authenticator {
    fn authenticate(&self, context: &AuthContext) -> Result<AuthDecision, FrontendError>;
}

#[derive(Clone, Debug, Default)]
pub struct StaticAuthenticator;

impl Authenticator for StaticAuthenticator {
    fn authenticate(&self, context: &AuthContext) -> Result<AuthDecision, FrontendError> {
        match context.method {
            AuthMethod::Trust => Ok(AuthDecision::allow(context)),
            AuthMethod::Password | AuthMethod::Token => {
                if context
                    .secret
                    .as_deref()
                    .is_some_and(|secret| !secret.is_empty())
                {
                    Ok(AuthDecision::allow(context))
                } else {
                    Err(FrontendError::AuthenticationFailed {
                        reason: "missing credentials".to_string(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthContext, AuthMethod, Authenticator, StaticAuthenticator};

    #[test]
    fn static_authenticator_rejects_empty_password() {
        let authenticator = StaticAuthenticator;
        let error = authenticator
            .authenticate(&AuthContext::new("brew", AuthMethod::Password))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "authentication failed: missing credentials"
        );
    }
}
