//! Shared request context and common utility types.

use crate::ids::{RequestId, SessionId};

/// Request-scoped context shared by entry and runtime layers.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct RequestContext {
    pub request_id: Option<RequestId>,
    pub session_id: Option<SessionId>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
}

impl RequestContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    pub fn with_session_id(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::RequestContext;
    use crate::ids::{RequestId, SessionId};

    #[test]
    fn request_context_builder_is_composable() {
        let context = RequestContext::new()
            .with_request_id(RequestId::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap())
            .with_session_id(SessionId::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap())
            .with_tenant_id("tenant-a")
            .with_user_id("user-a");

        assert_eq!(
            context.request_id.as_ref().map(|id| id.to_string()),
            Some("550e8400-e29b-41d4-a716-446655440000".to_owned())
        );
        assert_eq!(
            context.session_id.as_ref().map(|id| id.to_string()),
            Some("550e8400-e29b-41d4-a716-446655440001".to_owned())
        );
        assert_eq!(context.tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(context.user_id.as_deref(), Some("user-a"));
    }
}
