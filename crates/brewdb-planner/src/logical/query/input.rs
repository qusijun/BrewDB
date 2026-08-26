use crate::parser::ast::{SetExpr, Statement as AstStatement};
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::relation;
use crate::planner::PlannerError;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder};

pub(super) fn build_from_input(
    ast: &AstStatement,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let AstStatement::Query(query) = ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{ast}`"),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{}`", query.body),
        });
    };
    if select.from.is_empty() {
        return LogicalPlanBuilder::empty(true)
            .build()
            .map_err(map_df_plan_error);
    }
    let mut inputs = select
        .from
        .iter()
        .map(|from| relation::build_table_with_joins(from, planner_context))
        .collect::<Result<Vec<_>, _>>()?;
    let mut input = inputs.remove(0);
    for next in inputs {
        input = LogicalPlanBuilder::from(input)
            .cross_join(next)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    Ok(input)
}
