//! Prepared-statement and portal shell.

/// Frontend prepared statement placeholder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedStatement {
    pub statement_name: String,
    pub sql: String,
}

/// Frontend portal placeholder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Portal {
    pub portal_name: String,
    pub statement_name: String,
}
