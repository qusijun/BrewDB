//! Paimon dependency binding shell.

use paimon::{CatalogOptions, Options, catalog::Identifier};

use brewdb_catalog::model::TableRecord;
use brewdb_core::catalog::FormatType;

use crate::adapter::StorageAdapter;

/// Paimon-specific catalog binding assembled from normalized BrewDB metadata.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaimonCatalogBinding {
    pub warehouse_uri: String,
    pub table_identifier: Identifier,
}

impl PaimonCatalogBinding {
    #[allow(dead_code)]
    pub fn from_table_record(table: &TableRecord, warehouse_uri: impl Into<String>) -> Self {
        Self {
            warehouse_uri: warehouse_uri.into(),
            table_identifier: Identifier::new(
                table.table_ref.logical_name.database_name.clone(),
                table.table_ref.logical_name.table_name.clone(),
            ),
        }
    }

    #[allow(dead_code)]
    pub fn catalog_options(&self) -> Options {
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, self.warehouse_uri.clone());
        options
    }
}

/// Minimal adapter marker for the first concrete Paimon integration wave.
#[allow(dead_code)]
pub trait PaimonStorageAdapter: StorageAdapter {
    fn paimon_binding(&self, table: &TableRecord, warehouse_uri: &str) -> PaimonCatalogBinding {
        PaimonCatalogBinding::from_table_record(table, warehouse_uri.to_owned())
    }

    fn format_type(&self) -> FormatType {
        FormatType::Paimon
    }
}

#[cfg(test)]
mod tests {
    use brewdb_catalog::model::TableRecord;
    use brewdb_core::catalog::FormatType;
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::PaimonCatalogBinding;

    #[test]
    fn paimon_binding_maps_normalized_table_into_paimon_options() {
        let table = TableRecord::new(
            "brew",
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655441500").unwrap(),
            "analytics",
            "ns-analytics",
            TableId::parse_str("550e8400-e29b-41d4-a716-446655441501").unwrap(),
            "orders",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655441502").unwrap(),
            "warehouse-a",
            FormatType::Paimon,
        );

        let binding = PaimonCatalogBinding::from_table_record(&table, "s3://warehouse-a");
        let options = binding.catalog_options();

        assert_eq!(
            options.get(paimon::CatalogOptions::WAREHOUSE),
            Some(&"s3://warehouse-a".to_owned())
        );
        assert_eq!(binding.table_identifier.database(), "analytics");
        assert_eq!(binding.table_identifier.full_name(), "analytics.orders");
    }
}
