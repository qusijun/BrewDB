//! Catalog facade and normalized control-plane models for BrewDB.

pub mod errors;
pub mod facade;
pub mod model;
pub mod route;
pub mod warehouse;

pub(crate) mod cache;
pub(crate) mod client;
pub(crate) mod normalize;
