//! Shared catalog-facing domain types.

use crate::ids::{NamespaceId, TableId, WarehouseId};

/// Stable route key for table-format dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FormatType {
    Paimon,
    Iceberg,
}

/// A stable shared reference to one logical table identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub namespace_id: NamespaceId,
    pub table_id: TableId,
    pub warehouse_id: WarehouseId,
    pub format_type: FormatType,
}
