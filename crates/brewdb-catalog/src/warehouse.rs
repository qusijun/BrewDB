//! Warehouse and credential profile resolution.

use brewdb_core::ids::WarehouseId;

use crate::errors::CatalogError;
use crate::model::WarehouseProfile;

/// Warehouse lookup request from upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveWarehouseProfile {
    pub warehouse_id: WarehouseId,
}

/// Warehouse-profile resolution boundary.
pub trait WarehouseResolver {
    fn resolve_warehouse_profile(
        &self,
        request: ResolveWarehouseProfile,
    ) -> Result<WarehouseProfile, CatalogError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::ids::WarehouseId;

    use crate::model::WarehouseProfile;

    use super::ResolveWarehouseProfile;

    #[test]
    fn warehouse_resolution_shell_carries_profile_identity() {
        let request = ResolveWarehouseProfile {
            warehouse_id: WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441220").unwrap(),
        };
        let profile = WarehouseProfile {
            warehouse_id: request.warehouse_id.clone(),
            warehouse_name: "warehouse-a".to_owned(),
            default_uri: Some("s3://warehouse-a".to_owned()),
            credential_profile: Some("aws-prod".to_owned()),
        };

        assert_eq!(profile.warehouse_name, "warehouse-a");
        assert_eq!(profile.default_uri.as_deref(), Some("s3://warehouse-a"));
    }
}
