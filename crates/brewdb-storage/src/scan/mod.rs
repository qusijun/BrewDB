//! Scan requirement contracts.

use brewdb_core::catalog::TableRef;

use crate::errors::StorageError;

/// Scan planning input normalized for storage adapters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPlanningInput {
    pub table: TableRef,
    pub projected_columns: Vec<String>,
    pub predicate_summary: Option<String>,
}

/// Scan planning output consumed by optimizer and execution layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPlanningOutput {
    pub table: TableRef,
    pub required_partition_filters: Vec<String>,
    pub preserves_ordering: bool,
    pub preferred_partition_count: Option<u32>,
}

/// Storage scan planner boundary.
pub trait ScanPlanner {
    fn plan_scan(&self, input: ScanPlanningInput) -> Result<ScanPlanningOutput, StorageError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::{ScanPlanningInput, ScanPlanningOutput};

    fn table_ref() -> TableRef {
        TableRef::new(
            "brew",
            "analytics",
            "events",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655440912").unwrap(),
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655440910").unwrap(),
            TableId::parse_str("550e8400-e29b-41d4-a716-446655440911").unwrap(),
            FormatType::Paimon,
        )
    }

    #[test]
    fn scan_planning_shell_carries_projection_and_partition_preferences() {
        let input = ScanPlanningInput {
            table: table_ref(),
            projected_columns: vec!["id".to_owned(), "ts".to_owned()],
            predicate_summary: Some("ts >= 2026-01-01".to_owned()),
        };
        let output = ScanPlanningOutput {
            table: input.table.clone(),
            required_partition_filters: vec!["ts".to_owned()],
            preserves_ordering: false,
            preferred_partition_count: Some(16),
        };

        assert_eq!(input.projected_columns.len(), 2);
        assert_eq!(output.required_partition_filters, vec!["ts".to_owned()]);
    }
}
