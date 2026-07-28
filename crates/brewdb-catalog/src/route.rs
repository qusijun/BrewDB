//! Catalog routing and format-handle lookups.

use brewdb_core::catalog::{FormatType, TableRef};

use crate::errors::CatalogError;

/// Stable route lookup request from upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveRoute {
    pub table: TableRef,
}

/// Normalized route result for storage-adapter dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteResolution {
    pub table: TableRef,
    pub format_type: FormatType,
    pub route_key: String,
}

/// Route lookup boundary.
pub trait RouteResolver {
    fn resolve_route(&self, request: ResolveRoute) -> Result<RouteResolution, CatalogError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::{ResolveRoute, RouteResolution};

    #[test]
    fn route_resolution_keeps_dispatch_key() {
        let table = TableRef::new(
            "brew",
            "db1",
            "orders",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441212").unwrap(),
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441210").unwrap(),
            TableId::parse_str("550e8400-e29b-41d4-a716-446655441211").unwrap(),
            FormatType::Paimon,
        );
        let resolution = RouteResolution {
            table: table.clone(),
            format_type: table.format_type,
            route_key: "warehouse-a/paimon".to_owned(),
        };
        let request = ResolveRoute { table };

        assert_eq!(request.table.logical_name.catalog_name, "brew");
        assert_eq!(resolution.route_key, "warehouse-a/paimon");
    }
}
