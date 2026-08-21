//! BrewDB client frontend boundary.

pub mod common {
    pub use brewdb_common::common::*;
}

pub mod auth;
pub mod config;
pub mod errors;
pub mod pgwire;
pub mod portal;
pub mod protocol;
pub mod result;
pub mod session;

pub mod frontend {
    pub use crate::common::context::QueryContext;
    pub use crate::common::defaults::{DEFAULT_DATABASE_NAME, MANAGED_PAIMON_CATALOG_NAME};
    pub use crate::{auth, config, errors, pgwire, portal, protocol, result, session};
    pub use crate::{
        auth::*, config::*, errors::*, pgwire::*, portal::*, protocol::*, result::*, session::*,
    };
}

pub use frontend::*;
