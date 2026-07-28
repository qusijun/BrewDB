//! Append-like mutation contracts.

use brewdb_core::catalog::TableRef;
use brewdb_core::ids::StageId;

use crate::errors::StorageError;

/// Append planning input from runtime and execution layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendPlanningInput {
    pub table: TableRef,
    pub input_stage_id: Option<StageId>,
    pub requires_clustering: bool,
}

/// Append planning output consumed by execution and finalization layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendPlanningOutput {
    pub table: TableRef,
    pub required_sort_keys: Vec<String>,
    pub preferred_writer_count: Option<u32>,
    pub requires_staged_artifacts: bool,
}

/// Storage append planner boundary.
pub trait AppendPlanner {
    fn plan_append(&self, input: AppendPlanningInput)
    -> Result<AppendPlanningOutput, StorageError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{NamespaceId, StageId, TableId, WarehouseId};

    use super::{AppendPlanningInput, AppendPlanningOutput};

    fn table_ref() -> TableRef {
        TableRef::new(
            "brew",
            "analytics",
            "orders",
            WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655440922").unwrap(),
            NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655440920").unwrap(),
            TableId::parse_str("550e8400-e29b-41d4-a716-446655440921").unwrap(),
            FormatType::Iceberg,
        )
    }

    #[test]
    fn append_planning_shell_carries_writer_preferences() {
        let input = AppendPlanningInput {
            table: table_ref(),
            input_stage_id: Some(
                StageId::parse_str("550e8400-e29b-41d4-a716-446655440923").unwrap(),
            ),
            requires_clustering: true,
        };
        let output = AppendPlanningOutput {
            table: input.table.clone(),
            required_sort_keys: vec!["id".to_owned()],
            preferred_writer_count: Some(8),
            requires_staged_artifacts: true,
        };

        assert!(input.requires_clustering);
        assert_eq!(output.preferred_writer_count, Some(8));
    }
}
