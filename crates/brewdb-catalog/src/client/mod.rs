//! Internal control-plane client implementations.

use brewdb_core::ids::{TableId, WarehouseId};

use crate::errors::CatalogError;
use crate::model::{TableRecord, WarehouseProfile};

/// Control-plane client boundary for fetching normalized table metadata inputs.
pub trait CatalogClient {
    fn fetch_table(&self, table_id: &TableId) -> Result<TableRecord, CatalogError>;

    fn fetch_warehouse_profile(
        &self,
        warehouse_id: &WarehouseId,
    ) -> Result<WarehouseProfile, CatalogError>;
}
