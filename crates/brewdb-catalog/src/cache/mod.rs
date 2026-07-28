//! Internal catalog cache implementations.

use brewdb_core::ids::{TableId, WarehouseId};

use crate::errors::CatalogError;
use crate::model::{TableRecord, WarehouseProfile};

/// Cache lookup for normalized table metadata.
pub trait TableCache {
    fn get_table(&self, table_id: &TableId) -> Result<Option<TableRecord>, CatalogError>;
}

/// Cache lookup for normalized warehouse profiles.
pub trait WarehouseCache {
    fn get_warehouse_profile(
        &self,
        warehouse_id: &WarehouseId,
    ) -> Result<Option<WarehouseProfile>, CatalogError>;
}
