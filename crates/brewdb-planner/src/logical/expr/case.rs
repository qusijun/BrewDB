use crate::parser::ast::{CaseWhen, Expr as AstExpr};
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::{Case, Expr as DataFusionExpr};

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_case(
    operand: Option<&AstExpr>,
    conditions: &[CaseWhen],
    else_result: Option<&AstExpr>,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    Ok(DataFusionExpr::Case(Case::new(
        operand
            .map(|expr| {
                bind_expr_with_subqueries(expr, planner_context, subquery_planner).map(Box::new)
            })
            .transpose()?,
        conditions
            .iter()
            .map(|when| {
                Ok((
                    Box::new(bind_expr_with_subqueries(
                        &when.condition,
                        planner_context,
                        subquery_planner,
                    )?),
                    Box::new(bind_expr_with_subqueries(
                        &when.result,
                        planner_context,
                        subquery_planner,
                    )?),
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?,
        else_result
            .map(|expr| {
                bind_expr_with_subqueries(expr, planner_context, subquery_planner).map(Box::new)
            })
            .transpose()?,
    )))
}
