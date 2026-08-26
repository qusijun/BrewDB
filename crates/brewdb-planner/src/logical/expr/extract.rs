use crate::parser::ast::{DateTimeField, Expr as AstExpr};
use crate::planner::errors::{map_df_plan_error, PlannerError};
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_common::ScalarValue;
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::Expr as DataFusionExpr;

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_extract(
    field: &DateTimeField,
    expr: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let mut extract_args = vec![
        DataFusionExpr::Literal(ScalarValue::from(format!("{field}")), None),
        bind_expr_with_subqueries(expr, planner_context, subquery_planner)?,
    ];
    for planner in planner_context.expr_planners() {
        match planner
            .plan_extract(extract_args)
            .map_err(map_df_plan_error)?
        {
            PlannerResult::Planned(expr) => return Ok(expr),
            PlannerResult::Original(args) => extract_args = args,
        }
    }
    Err(PlannerError::UnsupportedPlan {
        reason: format!("extract could not be planned: {extract_args:?}"),
    })
}
