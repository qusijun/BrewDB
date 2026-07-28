//! Client-facing result shell.

use brewdb_core::ids::JobId;

/// Frontend result column descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub data_type: String,
}

/// Client-facing query result shell before wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResultEnvelope {
    pub job_id: JobId,
    pub columns: Vec<ResultColumn>,
    pub row_count: Option<u64>,
}
