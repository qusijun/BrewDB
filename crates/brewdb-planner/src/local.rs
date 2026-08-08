//! Node-local fragment planning contracts.

use std::sync::Arc;

use brewdb_catalog::TableCatalogEntry;
use brewdb_common::runtime::QueryContext;
use brewdb_storage::StorageEngine;
use datafusion::datasource::provider_as_source;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_optimizer::{ApplyOrder, Optimizer, OptimizerContext, OptimizerRule};

use crate::errors::PlannerError;
use crate::plan::{PlanFragment, PlanFragmentId, PlanFragmentKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFragmentPlan {
    pub query_context: QueryContext,
    pub fragment_id: PlanFragmentId,
    pub fragment_kind: PlanFragmentKind,
    pub logical_plan: DataFusionLogicalPlan,
}

impl LocalFragmentPlan {
    pub fn prepare(
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
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
        let optimizer = Optimizer::with_rules(vec![Arc::new(LocalTableScanRewriteRule {
            storage,
            tables: table_catalogs,
        })]);
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
        })
    }
}

#[derive(Clone)]
struct LocalTableScanRewriteRule {
    storage: Arc<dyn StorageEngine>,
    tables: Vec<TableCatalogEntry>,
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
                let Some(table) = self
                    .tables
                    .iter()
                    .find(|table| table.path.table() == scan.table_name.table())
                else {
                    return Ok(Transformed::no(DataFusionLogicalPlan::TableScan(scan)));
                };
                let provider = self
                    .storage
                    .table_engine(table)
                    .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?
                    .table_provider()
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
