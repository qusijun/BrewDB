//! Normalized catalog-facing metadata models.
//!
//! `TableCatalogEntry` is intentionally a control-plane object.
//! It carries stable table identity and format routing plus the table location
//! pointer needed to open the underlying storage catalog or table engine.
//! It does not cache format-native schema, snapshot, manifest, or file-level
//! metadata inside BrewDB's catalog store.

use std::collections::BTreeMap;

use crate::common::table::TableSchema;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use uuid::Uuid;

use crate::catalog::path::{CatalogPath, DatabasePath, TablePath};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogMode {
    Managed,
    Mounted,
    Temporary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageKind {
    Paimon,
    Iceberg,
    File,
    Memory,
}

impl StorageKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paimon => "paimon",
            Self::Iceberg => "iceberg",
            Self::File => "file",
            Self::Memory => "memory",
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
    pub storage_kind: StorageKind,
    pub options: BTreeMap<String, String>,
}

impl CatalogEntry {
    pub fn new(
        catalog_id: Uuid,
        path: CatalogPath,
        mode: CatalogMode,
        storage_kind: StorageKind,
    ) -> Self {
        Self {
            catalog_id,
            path,
            mode,
            storage_kind,
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
    pub storage_kind: StorageKind,
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
        storage_kind: StorageKind,
        catalog_mode: CatalogMode,
    ) -> Self {
        Self {
            table_id,
            catalog_id,
            database_id,
            path,
            table_schema,
            table_location: table_location.into(),
            storage_kind,
            catalog_mode,
            table_options: BTreeMap::new(),
            summary: TableSummary::default(),
        }
    }

    pub fn temporary_file(
        table_name: impl Into<String>,
        location: impl Into<String>,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self, crate::catalog::errors::CatalogError> {
        let table_options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        Ok(Self::new(
            Uuid::new_v4(),
            Uuid::nil(),
            Uuid::nil(),
            TablePath::new("__temporary", "__file", table_name.into())?,
            TableSchema::new(vec![]),
            location,
            StorageKind::File,
            CatalogMode::Temporary,
        )
        .with_options(table_options))
    }

    pub fn temporary_file_with_schema(
        table_name: impl Into<String>,
        location: impl Into<String>,
        table_schema: TableSchema,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self, crate::catalog::errors::CatalogError> {
        let table_options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        Ok(Self::new(
            Uuid::new_v4(),
            Uuid::nil(),
            Uuid::nil(),
            TablePath::new("__temporary", "__file", table_name.into())?,
            table_schema,
            location,
            StorageKind::File,
            CatalogMode::Temporary,
        )
        .with_options(table_options))
    }

    pub fn table_ref(&self) -> TableRef {
        TableRef::new(self.table_id)
    }

    pub fn table_stats_handle(&self) -> TableStatsHandle {
        TableStatsHandle::new(self.table_id)
    }

    pub fn with_primary_keys(
        mut self,
        primary_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.table_schema.primary_keys = primary_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_partition_keys(
        mut self,
        partition_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.table_schema.partition_keys = partition_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_bucket_keys(
        mut self,
        bucket_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.table_schema.bucket_keys = bucket_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_cluster_keys(
        mut self,
        cluster_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.table_schema.cluster_keys = cluster_keys.into_iter().map(Into::into).collect();
        self
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

#[cfg(test)]
mod tests {
    use super::{CatalogMode, StorageKind, TableCatalogEntry, TableStatsHandle};
    use crate::catalog::path::TablePath;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};

    #[test]
    fn table_catalog_entry_exposes_table_stats_handle() {
        let table_id = uuid::Uuid::new_v4();
        let entry = TableCatalogEntry::new(
            table_id,
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        );
        let handle = entry.table_stats_handle();
        assert_eq!(handle, TableStatsHandle::new(table_id));
        assert_eq!(handle.id(), table_id);
    }

    #[test]
    fn table_catalog_entry_keeps_layout_keys_in_table_schema() {
        let entry = TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("dt", DataType::String)]),
            "s3://warehouse/sales/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
        .with_primary_keys(["id"])
        .with_partition_keys(["dt"])
        .with_bucket_keys(["id"])
        .with_cluster_keys(["dt"]);

        assert_eq!(entry.table_schema.primary_keys, vec!["id"]);
        assert_eq!(entry.table_schema.partition_keys, vec!["dt"]);
        assert_eq!(entry.table_schema.bucket_keys, vec!["id"]);
        assert_eq!(entry.table_schema.cluster_keys, vec!["dt"]);
    }
}
