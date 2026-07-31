//! Canonical catalog.database.table naming helpers.

use std::fmt;

use crate::errors::CatalogError;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogPath {
    catalog: String,
}

impl CatalogPath {
    pub fn new(catalog: impl Into<String>) -> Result<Self, CatalogError> {
        let catalog = normalize_segment("catalog", catalog.into())?;
        Ok(Self { catalog })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }
}

impl fmt::Display for CatalogPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.catalog())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatabasePath {
    catalog: String,
    database: String,
}

impl DatabasePath {
    pub fn new(
        catalog: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        Ok(Self {
            catalog: normalize_segment("catalog", catalog.into())?,
            database: normalize_segment("database", database.into())?,
        })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn catalog_path(&self) -> CatalogPath {
        CatalogPath {
            catalog: self.catalog.clone(),
        }
    }
}

impl fmt::Display for DatabasePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.catalog(), self.database())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TablePath {
    catalog: String,
    database: String,
    table: String,
}

impl TablePath {
    pub fn new(
        catalog: impl Into<String>,
        database: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        Ok(Self {
            catalog: normalize_segment("catalog", catalog.into())?,
            database: normalize_segment("database", database.into())?,
            table: normalize_segment("table", table.into())?,
        })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn database_path(&self) -> DatabasePath {
        DatabasePath {
            catalog: self.catalog.clone(),
            database: self.database.clone(),
        }
    }
}

impl fmt::Display for TablePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog(), self.database(), self.table())
    }
}

fn normalize_segment(kind: &'static str, value: String) -> Result<String, CatalogError> {
    if value.is_empty() || value.contains('.') {
        return Err(CatalogError::InvalidPath { kind, value });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{CatalogPath, DatabasePath, TablePath};

    #[test]
    fn table_path_formats_as_catalog_database_table() {
        let path = TablePath::new("prod", "sales", "orders").unwrap();

        assert_eq!(path.to_string(), "prod.sales.orders");
        assert_eq!(path.database_path().to_string(), "prod.sales");
    }

    #[test]
    fn path_segments_reject_dotted_or_empty_names() {
        assert!(CatalogPath::new("").is_err());
        assert!(DatabasePath::new("prod", "sales.east").is_err());
        assert!(TablePath::new("prod", "sales", "orders.v1").is_err());
    }
}
