//! Normalized namespace, table, and warehouse models.

use brewdb_core::catalog::{FormatType, TableRef};
use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

/// Normalized namespace shell exposed to upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRecord {
    pub namespace_id: NamespaceId,
    pub database_name: String,
    pub namespace_name: String,
}

/// Normalized warehouse profile shell exposed to upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WarehouseProfile {
    pub warehouse_id: WarehouseId,
    pub warehouse_name: String,
    pub default_uri: Option<String>,
    pub credential_profile: Option<String>,
}

/// Normalized table record shell exposed to upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRecord {
    pub table_ref: TableRef,
    pub warehouse_name: String,
    pub namespace_name: String,
}

impl TableRecord {
    pub fn new(
        catalog_name: impl Into<String>,
        namespace_id: NamespaceId,
        database_name: impl Into<String>,
        namespace_name: impl Into<String>,
        table_id: TableId,
        table_name: impl Into<String>,
        warehouse_id: WarehouseId,
        warehouse_name: impl Into<String>,
        format_type: FormatType,
    ) -> Self {
        Self {
            table_ref: TableRef::new(
                catalog_name,
                database_name,
                table_name,
                warehouse_id,
                namespace_id,
                table_id,
                format_type,
            ),
            warehouse_name: warehouse_name.into(),
            namespace_name: namespace_name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::FormatType;
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::TableRecord;

    #[test]
    fn table_record_builds_shared_table_ref() {
        let table = TableRecord::new(
            "brew",
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441200").unwrap(),
            "analytics",
            "analytics",
            TableId::parse_str("550e8400-e29b-41d4-a716-446655441201").unwrap(),
            "orders",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441202").unwrap(),
            "warehouse-a",
            FormatType::Iceberg,
        );

        assert_eq!(table.table_ref.logical_name.table_name, "orders");
        assert_eq!(table.table_ref.logical_name.catalog_name, "brew");
        assert_eq!(table.table_ref.logical_name.database_name, "analytics");
        assert_eq!(table.warehouse_name, "warehouse-a");
        assert_eq!(table.namespace_name, "analytics");
        assert_eq!(table.table_ref.format_type, FormatType::Iceberg);
    }
}
