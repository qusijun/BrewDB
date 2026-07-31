//! SQL statement envelope contracts.

use crate::ingress::FrontendStatementRouteScope;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementCategory {
    Session,
    Runtime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionStatement {
    pub statement_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatement {
    pub statement_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatementPayload {
    Session(SessionStatement),
    Runtime(RuntimeStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlStatementEnvelope {
    pub statement_text: String,
    pub statement_name: String,
    pub category: StatementCategory,
    pub route_scope: FrontendStatementRouteScope,
    pub payload: StatementPayload,
}
