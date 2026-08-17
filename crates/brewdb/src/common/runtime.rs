//! Runtime-facing shared contracts.

use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryContext {
    pub query_id: Uuid,
}
