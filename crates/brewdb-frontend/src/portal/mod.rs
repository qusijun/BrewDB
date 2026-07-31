//! Prepared statement and portal shells.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PreparedStatementHandle {
    pub name: String,
    pub sql: String,
}

impl PreparedStatementHandle {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortalName(String);

impl PortalName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortalHandle {
    pub name: PortalName,
    pub statement_name: String,
}

impl PortalHandle {
    pub fn new(name: PortalName, statement_name: impl Into<String>) -> Self {
        Self {
            name,
            statement_name: statement_name.into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PortalCatalog {
    statements: BTreeMap<String, PreparedStatementHandle>,
    portals: BTreeMap<PortalName, PortalHandle>,
}

impl PortalCatalog {
    pub fn register_statement(&mut self, statement: PreparedStatementHandle) {
        self.statements.insert(statement.name.clone(), statement);
    }

    pub fn register_portal(&mut self, portal: PortalHandle) {
        self.portals.insert(portal.name.clone(), portal);
    }

    pub fn statement(&self, name: &str) -> Option<&PreparedStatementHandle> {
        self.statements.get(name)
    }

    pub fn portal(&self, name: &PortalName) -> Option<&PortalHandle> {
        self.portals.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{PortalCatalog, PortalHandle, PortalName, PreparedStatementHandle};

    #[test]
    fn portal_catalog_tracks_statement_and_portal() {
        let mut catalog = PortalCatalog::default();
        catalog.register_statement(PreparedStatementHandle::new("q1", "select 1"));
        catalog.register_portal(PortalHandle::new(PortalName::new("p1"), "q1"));

        assert_eq!(catalog.statement("q1").unwrap().sql, "select 1");
        assert_eq!(
            catalog
                .portal(&PortalName::new("p1"))
                .unwrap()
                .statement_name,
            "q1"
        );
    }
}
