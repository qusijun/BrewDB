//! BrewDB frontend session ingress and protocol boundary.

pub mod auth;
pub mod errors;
pub mod pgwire;
pub mod portal;
pub mod result;
pub mod session;

pub use auth::{AuthContext, AuthDecision, AuthMethod, Authenticator, StaticAuthenticator};
pub use errors::FrontendError;
pub use pgwire::{PgWireCodec, PgWireRequest, PgWireResponse};
pub use portal::{PortalCatalog, PortalHandle, PortalName, PreparedStatementHandle};
pub use result::{
    CommandTag, FrontendResponse, Notice, QueryResultKind, QueryResultOutput, ResultField,
};
pub use session::{
    ClientCapabilities, ClientConnectionContext, ClientContext, ClientDefaults, ClientIdentity,
    ClientSessionContext, ClientSqlRequest, FrontendService, OpenClientSession,
    OpenedClientSession, RequestContext,
};
