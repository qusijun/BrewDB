//! BrewDB logical optimizer entrypoint.

use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_optimizer::{
    ApplyOrder, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};

use crate::logical::plan::LogicalPlanNode;

#[derive(Debug)]
pub struct LogicalOptimizer {
    optimizer: Optimizer,
}

impl Default for LogicalOptimizer {
    fn default() -> Self {
        let mut rules = Optimizer::new().rules;
        rules.push(Arc::new(LogicalPlanExtensionRule));
        Self {
            optimizer: Optimizer::with_rules(rules),
        }
    }
}

impl LogicalOptimizer {
    pub fn optimize(&self, plan: DataFusionLogicalPlan) -> Result<DataFusionLogicalPlan> {
        self.optimize_with_observer(plan, |_, _| {})
    }

    pub fn optimize_with_observer<F>(
        &self,
        plan: DataFusionLogicalPlan,
        observer: F,
    ) -> Result<DataFusionLogicalPlan>
    where
        F: FnMut(&DataFusionLogicalPlan, &dyn OptimizerRule),
    {
        self.optimizer
            .optimize(plan, &OptimizerContext::new(), observer)
    }
}

#[derive(Debug)]
struct LogicalPlanExtensionRule;

impl OptimizerRule for LogicalPlanExtensionRule {
    fn name(&self) -> &str {
        "brewdb_logical_extension"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: DataFusionLogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<DataFusionLogicalPlan>, DataFusionError> {
        let DataFusionLogicalPlan::Extension(extension) = plan else {
            return Ok(Transformed::no(plan));
        };

        if extension.node.as_any().is::<LogicalPlanNode>() {
            return Ok(Transformed::no(DataFusionLogicalPlan::Extension(extension)));
        }

        Ok(Transformed::no(DataFusionLogicalPlan::Extension(extension)))
    }
}
