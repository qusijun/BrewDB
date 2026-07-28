//! Storage-facing statistics contracts for planning and optimization.

use brewdb_core::catalog::TableRef;

use crate::errors::StorageError;

/// Confidence score attached to storage statistics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatisticsConfidence {
    Exact,
    Approximate,
    Estimated,
    Unknown,
}

/// Per-column statistics shell surfaced to planning layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

/// Table or partition statistics shell used by optimizer and execution planning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableStatistics {
    pub table: TableRef,
    pub row_count: Option<u64>,
    pub total_bytes: Option<u64>,
    pub confidence: StatisticsConfidence,
    pub columns: Vec<ColumnStatistics>,
}

/// Statistics lookup request from upper layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveStatistics {
    pub table: TableRef,
    pub include_columns: Vec<String>,
}

/// Storage statistics provider boundary.
pub trait StatisticsProvider {
    fn resolve_statistics(
        &self,
        request: ResolveStatistics,
    ) -> Result<TableStatistics, StorageError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::{ColumnStatistics, StatisticsConfidence, TableStatistics};

    #[test]
    fn table_statistics_keep_confidence_and_columns() {
        let stats = TableStatistics {
            table: TableRef {
                namespace_id: NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655440900")
                    .unwrap(),
                table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655440901").unwrap(),
                warehouse_id: WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655440902")
                    .unwrap(),
                format_type: FormatType::Iceberg,
            },
            row_count: Some(42),
            total_bytes: Some(4096),
            confidence: StatisticsConfidence::Approximate,
            columns: vec![ColumnStatistics {
                column_name: "id".to_owned(),
                null_count: Some(0),
                distinct_count: Some(42),
                min_value: Some("1".to_owned()),
                max_value: Some("42".to_owned()),
            }],
        };

        assert_eq!(stats.columns.len(), 1);
        assert_eq!(stats.confidence, StatisticsConfidence::Approximate);
    }
}
