//! BrewDB catalog model, store, and service.

pub mod common {
    pub use brewdb_common::common::*;
}

pub mod backend;
pub mod catalogs;
pub mod config;
pub mod errors;
pub mod model;
pub mod paimon_schema;
pub mod path;
pub mod requests;
pub mod service;
pub mod storage_format_schema;
pub mod store;

pub mod catalog {
    pub use crate::{
        backend, catalogs, config, errors, model, paimon_schema, path, requests, service,
        storage_format_schema, store,
    };
    pub use crate::{
        backend::*, catalogs::*, config::*, errors::*, model::*, paimon_schema::*, path::*,
        requests::*, service::*, storage_format_schema::*, store::*,
    };
}

pub use catalog::*;
