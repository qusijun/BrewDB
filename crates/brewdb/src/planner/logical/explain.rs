use std::sync::Arc;

use crate::parser::ast::{AnalyzeFormatKind, Statement as AstStatement};
use crate::SqlError;
use datafusion_common::display::{PlanType, ToStringifiedPlan};
use datafusion_common::DFSchema;
use datafusion_expr::{Explain, LogicalPlan as DataFusionLogicalPlan};

use crate::planner::logical::{LogicalPlanner, LogicalPlanningContext};

pub(crate) fn bind_explain_statement(
    planner: &LogicalPlanner,
    statement: &AstStatement,
    format: Option<AnalyzeFormatKind>,
    ctx: &LogicalPlanningContext<'_>,
) -> Result<DataFusionLogicalPlan, SqlError> {
    let input = planner.plan(statement.clone(), ctx)?;
    let _ = format;
    let stringified_plans = vec![input.to_stringified(PlanType::InitialLogicalPlan)];
    Ok(DataFusionLogicalPlan::Explain(Explain {
        verbose: false,
        explain_format: datafusion_expr::logical_plan::ExplainFormat::Indent,
        plan: Arc::new(input),
        stringified_plans,
        schema: Arc::new(
            DFSchema::try_from(DataFusionLogicalPlan::explain_schema()).map_err(|error| {
                SqlError::InvalidRequest {
                    reason: error.to_string(),
                }
            })?,
        ),
        logical_optimization_succeeded: false,
    }))
}
