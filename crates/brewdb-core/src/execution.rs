//! Shared execution-facing state enums and identifiers.

/// Shared execution boundary kinds used across planning and execution crates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BoundaryKind {
    Exchange,
    Materialization,
    Selection,
}
