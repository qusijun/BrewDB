//! Storage semantics and format adapters for BrewDB.

pub mod adapter;
pub mod errors;
pub mod model;
pub mod route;

pub mod append;
pub mod commit;
pub mod maintenance;
pub mod rewrite;
pub mod scan;
pub mod statistics;

pub(crate) mod formats;
