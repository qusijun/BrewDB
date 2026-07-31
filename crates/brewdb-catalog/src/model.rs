//! Normalized catalog-facing metadata models.

use uuid::Uuid;

use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CatalogRef {
    catalog_id: Uuid,
}

impl CatalogRef {
    pub fn new(catalog_id: Uuid) -> Self {
        Self { catalog_id }
    }

    pub fn id(&self) -> Uuid {
        self.catalog_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseRef {
    database_id: Uuid,
}

impl DatabaseRef {
    pub fn new(database_id: Uuid) -> Self {
        Self { database_id }
    }

    pub fn id(&self) -> Uuid {
        self.database_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TableRef {
    table_id: Uuid,
}

impl TableRef {
    pub fn new(table_id: Uuid) -> Self {
        Self { table_id }
    }

    pub fn id(&self) -> Uuid {
        self.table_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    pub catalog_id: Uuid,
    pub path: CatalogPath,
}

impl CatalogEntry {
    pub fn new(catalog_id: Uuid, path: CatalogPath) -> Self {
        Self { catalog_id, path }
    }

    pub fn catalog_ref(&self) -> CatalogRef {
        CatalogRef::new(self.catalog_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseEntry {
    pub database_id: Uuid,
    pub path: DatabasePath,
}

impl DatabaseEntry {
    pub fn new(database_id: Uuid, path: DatabasePath) -> Self {
        Self { database_id, path }
    }

    pub fn database_ref(&self) -> DatabaseRef {
        DatabaseRef::new(self.database_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableFormat {
    Paimon,
    Iceberg,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageBinding {
    pub format: TableFormat,
    pub location: String,
}

impl StorageBinding {
    pub fn new(format: TableFormat, location: impl Into<String>) -> Self {
        Self {
            format,
            location: location.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableCatalogEntry {
    pub table_id: Uuid,
    pub path: TablePath,
    pub storage: StorageBinding,
}

impl TableCatalogEntry {
    pub fn new(table_id: Uuid, path: TablePath, storage: StorageBinding) -> Self {
        Self {
            table_id,
            path,
            storage,
        }
    }

    pub fn table_ref(&self) -> TableRef {
        TableRef::new(self.table_id)
    }
}
