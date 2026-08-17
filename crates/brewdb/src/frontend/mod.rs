//! BrewDB frontend session ingress and protocol boundary.

pub mod auth;
pub mod config;
pub mod errors;
pub mod ingress;
pub mod pgwire;
pub mod portal;
pub mod protocol;
pub mod result;
pub mod session;

pub use crate::common::defaults::{DEFAULT_DATABASE_NAME, MANAGED_PAIMON_CATALOG_NAME};
pub use auth::{AuthContext, AuthDecision, AuthMethod, Authenticator, StaticAuthenticator};
pub use config::{FrontendConfig, DEFAULT_CATALOG_KEY, PGWIRE_LISTEN_ADDR_KEY};
pub use errors::FrontendError;
pub use ingress::{SqlClientCapabilities, SqlIngressRequest, SqlRequestContext, SqlSessionContext};
pub use pgwire::{PgWireCodec, PgWireRequest, PgWireResponse};
pub use portal::{PortalCatalog, PortalHandle, PortalName, PreparedStatementHandle};
pub use protocol::{
    FrontendProtocolPlugin, FrontendProtocolRequest, FrontendProtocolResponse, ProtocolRegistry,
    SqlExecutionResult, SqlRequestHandler,
};
pub use result::{
    CommandTag, FrontendResponse, Notice, QueryResultKind, QueryResultOutput, ResultField,
};
pub use session::{
    ClientCapabilities, ClientConnectionContext, ClientContext, ClientDefaults, ClientIdentity,
    ClientSessionContext, FrontendService, OpenClientSession, OpenedClientSession, RequestContext,
    SqlRequest,
};
