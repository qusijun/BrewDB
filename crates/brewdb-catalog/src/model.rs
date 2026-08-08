//! Normalized catalog-facing metadata models.
//!
//! `TableCatalogEntry` is intentionally a control-plane object.
//! It carries stable table identity and format routing plus the table location
//! pointer needed to open the underlying lake-format catalog or table engine.
//! It does not cache format-native schema, snapshot, manifest, or file-level
//! metadata inside BrewDB's catalog store.

use std::collections::BTreeMap;

use brewdb_common::schema::TableSchema;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use datafusion_expr::TableSource;
use datafusion_expr::TableType;
use uuid::Uuid;

use crate::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogMode {
    Managed,
    Mounted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LakeFormatKind {
    Paimon,
    Iceberg,
}

impl LakeFormatKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paimon => "paimon",
            Self::Iceberg => "iceberg",
        }
    }
}

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TableStatsHandle {
    table_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TableSummary {
    pub row_count: Option<u64>,
    pub total_size_bytes: Option<u64>,
}

impl TableRef {
    pub fn new(table_id: Uuid) -> Self {
        Self { table_id }
    }

    pub fn id(&self) -> Uuid {
        self.table_id
    }
}

impl TableStatsHandle {
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
    pub mode: CatalogMode,
    pub lake_format_kind: LakeFormatKind,
    pub options: BTreeMap<String, String>,
}

impl CatalogEntry {
    pub fn new(
        catalog_id: Uuid,
        path: CatalogPath,
        mode: CatalogMode,
        lake_format_kind: LakeFormatKind,
    ) -> Self {
        Self {
            catalog_id,
            path,
            mode,
            lake_format_kind,
            options: BTreeMap::new(),
        }
    }

    pub fn catalog_ref(&self) -> CatalogRef {
        CatalogRef::new(self.catalog_id)
    }

    pub fn with_options(
        mut self,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseCatalogEntry {
    pub database_id: Uuid,
    pub catalog_id: Uuid,
    pub path: DatabasePath,
    pub options: BTreeMap<String, String>,
}

impl DatabaseCatalogEntry {
    pub fn new(database_id: Uuid, catalog_id: Uuid, path: DatabasePath) -> Self {
        Self {
            database_id,
            catalog_id,
            path,
            options: BTreeMap::new(),
        }
    }

    pub fn database_ref(&self) -> DatabaseRef {
        DatabaseRef::new(self.database_id)
    }

    pub fn with_options(
        mut self,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableCatalogEntry {
    pub table_id: Uuid,
    pub catalog_id: Uuid,
    pub database_id: Uuid,
    pub path: TablePath,
    pub table_schema: TableSchema,
    /// Stable table root location owned by the underlying table format.
    pub table_location: String,
    pub lake_format_kind: LakeFormatKind,
    pub catalog_mode: CatalogMode,
    pub table_options: BTreeMap<String, String>,
    pub summary: TableSummary,
}

impl TableCatalogEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: Uuid,
        catalog_id: Uuid,
        database_id: Uuid,
        path: TablePath,
        table_schema: TableSchema,
        table_location: impl Into<String>,
        lake_format_kind: LakeFormatKind,
        catalog_mode: CatalogMode,
    ) -> Self {
        Self {
            table_id,
            catalog_id,
            database_id,
            path,
            table_schema,
            table_location: table_location.into(),
            lake_format_kind,
            catalog_mode,
            table_options: BTreeMap::new(),
            summary: TableSummary::default(),
        }
    }

    pub fn table_ref(&self) -> TableRef {
        TableRef::new(self.table_id)
    }

    pub fn table_stats_handle(&self) -> TableStatsHandle {
        TableStatsHandle::new(self.table_id)
    }

    pub fn with_options(
        mut self,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.table_options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    pub fn with_summary(mut self, summary: TableSummary) -> Self {
        self.summary = summary;
        self
    }

    pub fn to_datafusion_statistics(&self) -> Statistics {
        let schema = self
            .table_schema
            .to_arrow_schema_ref()
            .expect("catalog table schema must be convertible to Arrow");
        let mut stats = Statistics::new_unknown(schema.as_ref());
        if let Some(row_count) = self.summary.row_count {
            stats.num_rows = Precision::Exact(row_count as usize);
        }
        if let Some(total_size_bytes) = self.summary.total_size_bytes {
            stats.total_byte_size = Precision::Exact(total_size_bytes as usize);
        }
        stats
    }
}

impl TableSource for TableCatalogEntry {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.table_schema
            .to_arrow_schema_ref()
            .expect("catalog table schema must be convertible to Arrow")
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod tests {
    use super::{CatalogMode, LakeFormatKind, TableCatalogEntry, TableStatsHandle};
    use crate::path::TablePath;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};

    #[test]
    fn table_catalog_entry_exposes_table_stats_handle() {
        let table_id = uuid::Uuid::new_v4();
        let entry = TableCatalogEntry::new(
            table_id,
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            LakeFormatKind::Paimon,
            CatalogMode::Managed,
        );
        let handle = entry.table_stats_handle();
        assert_eq!(handle, TableStatsHandle::new(table_id));
        assert_eq!(handle.id(), table_id);
    }
}
