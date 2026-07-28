//! Internal control-plane client implementations.

pub mod local;
pub mod rest;

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

/// Reserved expansion slot for a future typed RPC client implementation.
///
/// Keep this boundary in `client/` so transport shape stays below facade/normalize.
pub trait CatalogRpcClient: CatalogClient {}
