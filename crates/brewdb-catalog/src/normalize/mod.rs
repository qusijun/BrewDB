//! Internal normalization from control-plane responses into BrewDB models.

use crate::errors::CatalogError;
use crate::model::{TableRecord, WarehouseProfile};

/// Normalization boundary from external control-plane responses into BrewDB models.
pub trait CatalogNormalizer {
    type RawTable;
    type RawWarehouseProfile;

    fn normalize_table(&self, raw: Self::RawTable) -> Result<TableRecord, CatalogError>;

    fn normalize_warehouse_profile(
        &self,
        raw: Self::RawWarehouseProfile,
    ) -> Result<WarehouseProfile, CatalogError>;
}
