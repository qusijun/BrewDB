//! SQL statement envelope contracts.

use crate::ingress::FrontendStatementRouteScope;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementCategory {
    Session,
    Runtime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatementPayload {
    Session(SessionStatement),
    Runtime(RuntimeStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlStatementEnvelope {
    pub statement_text: String,
    pub statement_name: Option<String>,
    pub category: StatementCategory,
    pub route_scope: FrontendStatementRouteScope,
    pub payload: StatementPayload,
}
