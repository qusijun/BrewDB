use crate::parser::ast::Expr as AstExpr;
use crate::planner::errors::{map_df_plan_error, PlannerError};
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_common::ScalarValue;
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::Expr as DataFusionExpr;

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_substring(
    expr: &AstExpr,
    substring_from: &Option<Box<AstExpr>>,
    substring_for: &Option<Box<AstExpr>>,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let mut substring_args = match (substring_from, substring_for) {
        (Some(from), Some(for_expr)) => {
            let arg = bind_expr_with_subqueries(expr, planner_context, subquery_planner)?;
            let from_logic = bind_expr_with_subqueries(from, planner_context, subquery_planner)?;
            let for_logic = bind_expr_with_subqueries(for_expr, planner_context, subquery_planner)?;
            vec![arg, from_logic, for_logic]
        }
        (Some(from), None) => {
            let arg = bind_expr_with_subqueries(expr, planner_context, subquery_planner)?;
            let from_logic = bind_expr_with_subqueries(from, planner_context, subquery_planner)?;
            vec![arg, from_logic]
        }
        (None, Some(for_expr)) => {
            let arg = bind_expr_with_subqueries(expr, planner_context, subquery_planner)?;
            let from_logic = DataFusionExpr::Literal(ScalarValue::Int64(Some(1)), None);
            let for_logic = bind_expr_with_subqueries(for_expr, planner_context, subquery_planner)?;
            vec![arg, from_logic, for_logic]
        }
        (None, None) => {
            return Err(PlannerError::InvalidPlan {
                reason: format!("substring without FROM or FOR is not valid: `{expr}`"),
            });
        }
    };
    for planner in planner_context.expr_planners() {
        match planner
            .plan_substring(substring_args)
            .map_err(map_df_plan_error)?
        {
            PlannerResult::Planned(expr) => return Ok(expr),
            PlannerResult::Original(args) => substring_args = args,
        }
    }
    Err(PlannerError::UnsupportedPlan {
        reason: format!("substring could not be planned: {substring_args:?}"),
    })
}
