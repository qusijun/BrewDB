//! BrewDB logical optimizer entrypoint.

use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, ApplyOrder, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};

use crate::common::context::QueryContext;
use crate::planner::logical::plan::LogicalPlanNode;

#[derive(Debug)]
pub struct LogicalOptimizer {
    analyzer: Analyzer,
    optimizer: Optimizer,
}

impl Default for LogicalOptimizer {
    fn default() -> Self {
        let mut rules = Optimizer::new().rules;
        rules.push(Arc::new(LogicalPlanExtensionRule));
        Self {
            analyzer: Analyzer::new(),
            optimizer: Optimizer::with_rules(rules),
        }
    }
}

impl LogicalOptimizer {
    pub fn optimize(&self, plan: DataFusionLogicalPlan) -> Result<DataFusionLogicalPlan> {
        self.optimize_with_observer(plan, |_, _| {})
    }

    pub fn optimize_with_query_context(
        &self,
        plan: DataFusionLogicalPlan,
        query_context: &QueryContext,
    ) -> Result<DataFusionLogicalPlan> {
        self.optimize_with_query_context_and_observers(query_context, plan, |_, _| {}, |_, _| {})
    }

    pub fn optimize_with_observer<F>(
        &self,
        plan: DataFusionLogicalPlan,
        observer: F,
    ) -> Result<DataFusionLogicalPlan>
    where
        F: FnMut(&DataFusionLogicalPlan, &dyn OptimizerRule),
    {
        self.optimize_with_observers(plan, |_, _| {}, observer)
    }

    pub fn optimize_with_observers<A, O>(
        &self,
        plan: DataFusionLogicalPlan,
        analyzer_observer: A,
        optimizer_observer: O,
    ) -> Result<DataFusionLogicalPlan>
    where
        A: FnMut(&DataFusionLogicalPlan, &dyn AnalyzerRule),
        O: FnMut(&DataFusionLogicalPlan, &dyn OptimizerRule),
    {
        let optimizer_context = OptimizerContext::new();
        self.optimize_with_context(
            plan,
            optimizer_context,
            analyzer_observer,
            optimizer_observer,
        )
    }

    pub fn optimize_with_query_context_and_observers<A, O>(
        &self,
        query_context: &QueryContext,
        plan: DataFusionLogicalPlan,
        analyzer_observer: A,
        optimizer_observer: O,
    ) -> Result<DataFusionLogicalPlan>
    where
        A: FnMut(&DataFusionLogicalPlan, &dyn AnalyzerRule),
        O: FnMut(&DataFusionLogicalPlan, &dyn OptimizerRule),
    {
        let optimizer_context =
            crate::runtime::datafusion_context::optimizer_context(query_context)?;
        self.optimize_with_context(
            plan,
            optimizer_context,
            analyzer_observer,
            optimizer_observer,
        )
    }

    fn optimize_with_context<A, O>(
        &self,
        plan: DataFusionLogicalPlan,
        optimizer_context: OptimizerContext,
        analyzer_observer: A,
        optimizer_observer: O,
    ) -> Result<DataFusionLogicalPlan>
    where
        A: FnMut(&DataFusionLogicalPlan, &dyn AnalyzerRule),
        O: FnMut(&DataFusionLogicalPlan, &dyn OptimizerRule),
    {
        let options = optimizer_context.options();
        let analyzed = self
            .analyzer
            .execute_and_check(plan, &options, analyzer_observer)?;
        self.optimizer
            .optimize(analyzed, &optimizer_context, optimizer_observer)
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
