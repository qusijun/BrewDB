//! Rewrite-mutation realization contracts.

use brewdb_core::catalog::TableRef;

use crate::errors::StorageError;

/// Rewrite planning input from mutation and maintenance workflows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewritePlanningInput {
    pub table: TableRef,
    pub candidate_scope_summary: Option<String>,
    pub row_level_changes: bool,
}

/// Rewrite planning output consumed by execution and finalization layers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewritePlanningOutput {
    pub table: TableRef,
    pub requires_selection_boundary: bool,
    pub emits_delete_artifacts: bool,
    pub emits_replacement_artifacts: bool,
}

/// Storage rewrite planner boundary.
pub trait RewritePlanner {
    fn plan_rewrite(
        &self,
        input: RewritePlanningInput,
    ) -> Result<RewritePlanningOutput, StorageError>;
}

#[cfg(test)]
mod tests {
    use brewdb_core::catalog::{FormatType, TableRef};
    use brewdb_core::ids::{NamespaceId, TableId, WarehouseId};

    use super::{RewritePlanningInput, RewritePlanningOutput};

    fn table_ref() -> TableRef {
        TableRef {
            namespace_id: NamespaceId::parse_str("550e8400-e29b-41d4-a716-446655440930").unwrap(),
            table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655440931").unwrap(),
            warehouse_id: WarehouseId::parse_str("550e8400-e29b-41d4-a716-446655440932").unwrap(),
            format_type: FormatType::Paimon,
        }
    }

    #[test]
    fn rewrite_planning_shell_carries_artifact_expectations() {
        let input = RewritePlanningInput {
            table: table_ref(),
            candidate_scope_summary: Some("partitions=2".to_owned()),
            row_level_changes: true,
        };
        let output = RewritePlanningOutput {
            table: input.table.clone(),
            requires_selection_boundary: true,
            emits_delete_artifacts: true,
            emits_replacement_artifacts: true,
        };

        assert!(input.row_level_changes);
        assert!(output.emits_replacement_artifacts);
    }
}
