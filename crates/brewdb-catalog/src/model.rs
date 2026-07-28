//! Normalized namespace, table, and warehouse models.

use brewdb_core::catalog::{FormatType, TableRef};
use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

/// Normalized namespace shell exposed to upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRecord {
    pub namespace_id: NamespaceId,
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
    pub table_name: String,
    pub namespace_name: String,
}

impl TableRecord {
    pub fn new(
        namespace_id: NamespaceId,
        namespace_name: impl Into<String>,
        table_id: TableId,
        table_name: impl Into<String>,
        warehouse_id: WarehouseId,
        format_type: FormatType,
    ) -> Self {
        Self {
            table_ref: TableRef {
                namespace_id,
                table_id,
                warehouse_id,
                format_type,
            },
            table_name: table_name.into(),
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
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441200").unwrap(),
            "analytics",
            TableId::parse_str("550e8400-e29b-41d4-a716-446655441201").unwrap(),
            "orders",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441202").unwrap(),
            FormatType::Iceberg,
        );

        assert_eq!(table.table_name, "orders");
        assert_eq!(table.namespace_name, "analytics");
        assert_eq!(table.table_ref.format_type, FormatType::Iceberg);
    }
}
