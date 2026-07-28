//! Shared catalog-facing domain types.

use crate::ids::{NamespaceId, TableId, WarehouseId};

/// Stable route key for table-format dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FormatType {
    Paimon,
    Iceberg,
}

/// SQL-facing logical table name using catalog.database.table layering.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTableName {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
}

/// Control-plane routing reference using warehouse.namespace.table identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ControlPlaneTableRef {
    pub table_id: TableId,
    pub namespace_id: NamespaceId,
    pub warehouse_id: WarehouseId,
}

/// A stable shared reference combining SQL-facing logical naming with control-plane routing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub logical_name: LogicalTableName,
    pub control_plane_ref: ControlPlaneTableRef,
    pub format_type: FormatType,
}

impl TableRef {
    pub fn new(
        catalog_name: impl Into<String>,
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        warehouse_id: WarehouseId,
        namespace_id: NamespaceId,
        table_id: TableId,
        format_type: FormatType,
    ) -> Self {
        Self {
            logical_name: LogicalTableName {
                catalog_name: catalog_name.into(),
                database_name: database_name.into(),
                table_name: table_name.into(),
            },
            control_plane_ref: ControlPlaneTableRef {
                table_id,
                namespace_id,
                warehouse_id,
            },
            format_type,
        }
    }
}
