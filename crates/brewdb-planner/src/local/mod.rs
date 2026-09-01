//! Node-local fragment planning contracts.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::common::context::QueryContext;
use crate::storage::StorageEngine;
use datafusion::datasource::provider_as_source;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_optimizer::{ApplyOrder, Optimizer, OptimizerRule};

use crate::planner::distributed::{PlanFragment, PlanFragmentId, PlanFragmentKind};
use crate::planner::errors::PlannerError;
use crate::planner::logical::table_source::DefaultTableSource;
use crate::storage::{TableScanSplit, TableScanSplitGroup, TableSourceId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFragmentPlan {
    pub query_context: QueryContext,
    pub fragment_id: PlanFragmentId,
    pub fragment_kind: PlanFragmentKind,
    pub logical_plan: DataFusionLogicalPlan,
    /// Local scan assignments used to rebuild this fragment's table providers.
    ///
    /// This is execution context, not a stable fragment plan description.
    /// Distributed workers get a group containing the instance's single split;
    /// standalone root execution can get the root fragment's whole scan group.
    pub local_scan_assignment: Option<TableScanSplitGroup>,
}

impl LocalFragmentPlan {
    pub fn prepare(
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
        local_scan_assignment: Option<TableScanSplitGroup>,
        storage: Arc<StorageEngine>,
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
                local_scan_assignment: local_scan_assignment.clone(),
                next_table_source_id: Arc::new(AtomicU32::new(0)),
            }),
            Arc::new(LocalDmlTargetRewriteRule {
                storage,
                tables: table_catalogs,
            }),
        ]);
        let optimizer_context =
            crate::planner::logical::optimizer::optimizer_context(&query_context)?;
        let optimized = optimizer.optimize(logical_plan, &optimizer_context, |_, _| {})?;
        Ok(Self {
            query_context,
            fragment_id: fragment.fragment_id,
            fragment_kind: fragment.kind,
            logical_plan: optimized,
            local_scan_assignment,
        })
    }
}

#[derive(Clone)]
struct LocalTableScanRewriteRule {
    storage: Arc<StorageEngine>,
    tables: Vec<TableCatalogEntry>,
    local_scan_assignment: Option<TableScanSplitGroup>,
    next_table_source_id: Arc<AtomicU32>,
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
                let table_source_id = self.next_table_source_id();
                let default_source = scan.source.downcast_ref::<DefaultTableSource>();
                let table_from_source = default_source.map(DefaultTableSource::table);
                let table_from_catalog = self
                    .tables
                    .iter()
                    .find(|table| table.path.table() == scan.table_name.table());
                let Some(table) = table_from_source.or(table_from_catalog) else {
                    return Ok(Transformed::no(DataFusionLogicalPlan::TableScan(scan)));
                };
                let assigned_split = self.assigned_split(table_source_id, &scan.table_name);
                let table_engine = if let Some(default_source) = default_source {
                    Arc::clone(default_source.table_engine())
                } else {
                    self.storage.table_engine(table).map_err(|err| {
                        datafusion_common::DataFusionError::External(Box::new(err))
                    })?
                };
                let provider = table_engine
                    .get_table_provider(assigned_split)
                    .map_err(|err| datafusion_common::DataFusionError::External(Box::new(err)))?;
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

impl LocalTableScanRewriteRule {
    fn next_table_source_id(&self) -> TableSourceId {
        // Local rewrite visits table scans in the same bottom-up order used by
        // distributed split planning. The current planner assigns dense ids
        // from zero while walking the logical plan inputs.
        TableSourceId(self.next_table_source_id.fetch_add(1, Ordering::Relaxed))
    }

    fn assigned_split(
        &self,
        table_source_id: TableSourceId,
        table_name: &datafusion_common::TableReference,
    ) -> Option<&TableScanSplit> {
        self.local_scan_assignment
            .as_ref()
            .and_then(|splits| {
                splits
                    .splits_for_table_source(table_source_id)
                    .or_else(|| splits.only_table_source_splits())
            })
            .and_then(|splits| {
                let [split] = splits else {
                    return None;
                };
                (split.table_name == table_name.to_string()).then_some(split)
            })
    }
}

#[derive(Clone)]
struct LocalDmlTargetRewriteRule {
    storage: Arc<StorageEngine>,
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
            .map_err(|err| datafusion_common::DataFusionError::External(Box::new(err)))?
            .table_provider()
            .map_err(|err| datafusion_common::DataFusionError::External(Box::new(err)))?;
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
    use brewdb_common::test_util::TestFile;

    #[test]
    fn file_table_engine_is_backed_by_storage_file_provider() {
        let path = TestFile::new("brewdb-local-file", "csv");
        std::fs::write(path.path(), "id\n1\n").unwrap();
        let engine = crate::storage::file::FileTableEngine::try_new(
            path.path().to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        let provider = engine.table_provider().unwrap();
        assert_eq!(provider.table_type(), datafusion_expr::TableType::Base);
    }
}
