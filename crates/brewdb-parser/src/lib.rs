#![cfg_attr(not(feature = "std"), no_std)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::large_enum_variant)]
#![forbid(clippy::unreachable)]
#![forbid(missing_docs)]

//! BrewDB SQL parser.

extern crate self as sqlparser;

#[cfg(not(feature = "std"))]
extern crate alloc;

/// Shared BrewDB common contracts used by parser diagnostics.
pub mod common {
    pub use brewdb_common::common::*;
}

/// SQL abstract syntax tree.
pub mod ast;
/// SQL dialect definitions.
#[macro_use]
pub mod dialect;
/// Display helpers for SQL AST rendering.
pub mod display_utils;
/// SQL keyword definitions.
pub mod keywords;
/// SQL parser implementation.
pub mod parser;
/// SQL tokenizer implementation.
pub mod tokenizer;

#[cfg(test)]
/// Parser test utilities.
pub mod test_utils;

#[cfg(feature = "derive-dialect")]
pub use dialect::derive_dialect;
pub use parser::{IsOptional, Parser, ParserError, ParserOptions};
