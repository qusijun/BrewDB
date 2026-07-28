//! Stable control-plane facade entry points.

use brewdb_core::ids::{TableId, WarehouseId};

use crate::errors::CatalogError;
use crate::model::{TableRecord, WarehouseProfile};
use crate::route::{ResolveRoute, RouteResolution};

/// Top-level table lookup request from upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveTable {
    pub table_id: TableId,
}

/// Top-level normalized metadata lookup request from upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveNormalizedMetadata {
    pub table_id: TableId,
}

/// Top-level catalog facade boundary used by planning and finalization.
pub trait CatalogFacade {
    fn resolve_table(&self, request: ResolveTable) -> Result<TableRecord, CatalogError>;

    fn resolve_warehouse_profile(
        &self,
        warehouse_id: WarehouseId,
    ) -> Result<WarehouseProfile, CatalogError>;

    fn resolve_route(&self, request: ResolveRoute) -> Result<RouteResolution, CatalogError>;

    fn resolve_normalized_metadata(
        &self,
        request: ResolveNormalizedMetadata,
    ) -> Result<TableRecord, CatalogError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::FormatType;
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use crate::model::TableRecord;
    use crate::route::{ResolveRoute, RouteResolution};

    use super::{ResolveNormalizedMetadata, ResolveTable};

    #[test]
    fn facade_requests_keep_lookup_identity() {
        let table_id = TableId::parse_str("550e8400-e29b-41d4-a716-446655441230").unwrap();
        let resolve_table = ResolveTable {
            table_id: table_id.clone(),
        };
        let resolve_metadata = ResolveNormalizedMetadata {
            table_id: table_id.clone(),
        };
        let record = TableRecord::new(
            "brew",
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441231").unwrap(),
            "analytics",
            "analytics",
            table_id.clone(),
            "events",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441232").unwrap(),
            "warehouse-a",
            FormatType::Iceberg,
        );
        let route = RouteResolution {
            table: record.table_ref.clone(),
            format_type: FormatType::Iceberg,
            route_key: "warehouse-a/iceberg".to_owned(),
        };
        let route_request = ResolveRoute {
            table: record.table_ref.clone(),
        };

        assert_eq!(resolve_table.table_id, table_id);
        assert_eq!(resolve_metadata.table_id, table_id);
        assert_eq!(
            route_request.table.control_plane_ref.table_id,
            route.table.control_plane_ref.table_id
        );
    }
}
