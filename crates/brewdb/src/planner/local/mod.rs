//! Node-local fragment planning contracts.

use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::common::runtime::QueryContext;
use crate::storage::StorageEngine;
use datafusion::datasource::provider_as_source;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_optimizer::{ApplyOrder, Optimizer, OptimizerContext, OptimizerRule};

use crate::planner::distributed::plan::{PlanFragment, PlanFragmentId, PlanFragmentKind};
use crate::planner::distributed::split::TableScanSplitGroup;
use crate::planner::errors::PlannerError;
use crate::planner::logical::table_source::DefaultTableSource;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFragmentPlan {
    pub query_context: QueryContext,
    pub fragment_id: PlanFragmentId,
    pub fragment_kind: PlanFragmentKind,
    pub logical_plan: DataFusionLogicalPlan,
    pub table_scan_splits: TableScanSplitGroup,
}

impl LocalFragmentPlan {
    pub fn prepare(
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
        table_scan_splits: TableScanSplitGroup,
        storage: Arc<dyn StorageEngine>,
    ) -> Result<Self, PlannerError> {
        let logical_plan = fragment
            .local_plan
            .ok_or_else(|| PlannerError::InvalidPlan {
                reason: format!(
                    "fragment {:?} is missing a local plan",
                    fragment.fragment_id
                ),
            })?;
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(LocalTableScanRewriteRule {
                storage: Arc::clone(&storage),
                tables: table_catalogs.clone(),
                table_scan_splits: table_scan_splits.clone(),
            }),
            Arc::new(LocalDmlTargetRewriteRule {
                storage,
                tables: table_catalogs,
            }),
        ]);
        let optimized = optimizer
            .optimize(logical_plan, &OptimizerContext::new(), |_, _| {})
            .map_err(|err| PlannerError::InvalidPlan {
                reason: err.to_string(),
            })?;
        Ok(Self {
            query_context,
            fragment_id: fragment.fragment_id,
            fragment_kind: fragment.kind,
            logical_plan: optimized,
            table_scan_splits,
        })
    }
}

#[derive(Clone)]
struct LocalTableScanRewriteRule {
    storage: Arc<dyn StorageEngine>,
    tables: Vec<TableCatalogEntry>,
    table_scan_splits: TableScanSplitGroup,
}

impl std::fmt::Debug for LocalTableScanRewriteRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocalTableScanRewriteRule")
    }
}

impl OptimizerRule for LocalTableScanRewriteRule {
    fn name(&self) -> &str {
        "LocalTableScanRewriteRule"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: DataFusionLogicalPlan,
        _config: &dyn datafusion_optimizer::OptimizerConfig,
    ) -> Result<Transformed<DataFusionLogicalPlan>, datafusion_common::DataFusionError> {
        match plan {
            DataFusionLogicalPlan::TableScan(scan) => {
                let default_source = scan.source.downcast_ref::<DefaultTableSource>();
                let table_from_source = default_source.map(DefaultTableSource::table);
                let table_from_catalog = self
                    .tables
                    .iter()
                    .find(|table| table.path.table() == scan.table_name.table());
                let Some(table) = table_from_source.or(table_from_catalog) else {
                    return Ok(Transformed::no(DataFusionLogicalPlan::TableScan(scan)));
                };
                let assigned_splits = self
                    .table_scan_splits
                    .for_table(&scan.table_name.to_string());
                let table_engine = match default_source.and_then(DefaultTableSource::table_engine) {
                    Some(table_engine) => Arc::clone(table_engine),
                    None => self
                        .storage
                        .table_engine(table)
                        .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?,
                };
                let provider = table_engine
                    .get_table_provider(&assigned_splits)
                    .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?;
                let rebuilt = datafusion_expr::TableScan::try_new(
                    scan.table_name.clone(),
                    provider_as_source(provider),
                    scan.projection.clone(),
                    scan.filters.clone(),
                    scan.fetch,
                )?;
                Ok(Transformed::yes(DataFusionLogicalPlan::TableScan(rebuilt)))
            }
            other => Ok(Transformed::no(other)),
        }
    }
}

#[derive(Clone)]
struct LocalDmlTargetRewriteRule {
    storage: Arc<dyn StorageEngine>,
    tables: Vec<TableCatalogEntry>,
}

impl std::fmt::Debug for LocalDmlTargetRewriteRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LocalDmlTargetRewriteRule")
    }
}

impl OptimizerRule for LocalDmlTargetRewriteRule {
    fn name(&self) -> &str {
        "LocalDmlTargetRewriteRule"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: DataFusionLogicalPlan,
        _config: &dyn datafusion_optimizer::OptimizerConfig,
    ) -> Result<Transformed<DataFusionLogicalPlan>, datafusion_common::DataFusionError> {
        let DataFusionLogicalPlan::Dml(dml) = plan else {
            return Ok(Transformed::no(plan));
        };
        let Some(table) = self
            .tables
            .iter()
            .find(|table| table.path.table() == dml.table_name.table())
        else {
            return Ok(Transformed::no(DataFusionLogicalPlan::Dml(dml)));
        };
        let provider = self
            .storage
            .table_engine(table)
            .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?
            .table_provider()
            .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?;
        let rebuilt = datafusion_expr::logical_plan::dml::DmlStatement::new(
            dml.table_name,
            provider_as_source(provider),
            dml.op,
            dml.input,
        );
        Ok(Transformed::yes(DataFusionLogicalPlan::Dml(rebuilt)))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::TableEngine;

    #[test]
    fn file_table_engine_is_backed_by_storage_file_provider() {
        let path =
            std::env::temp_dir().join(format!("brewdb-local-file-{}.csv", uuid::Uuid::new_v4()));
        std::fs::write(&path, "id\n1\n").unwrap();
        let engine = crate::storage::file::FileTableEngine::try_new(
            path.to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        let provider = engine.table_provider().unwrap();
        assert_eq!(provider.table_type(), datafusion_expr::TableType::Base);
    }
}
