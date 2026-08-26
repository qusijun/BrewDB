use crate::parser::ast::Expr as AstExpr;
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::expr::InList;
use datafusion_expr::{Between, Expr as DataFusionExpr};

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_is_null(
    expr: &AstExpr,
    negated: bool,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let expr = Box::new(bind_expr_with_subqueries(
        expr,
        planner_context,
        subquery_planner,
    )?);
    if negated {
        Ok(DataFusionExpr::IsNotNull(expr))
    } else {
        Ok(DataFusionExpr::IsNull(expr))
    }
}

pub(super) fn bind_between(
    expr: &AstExpr,
    negated: bool,
    low: &AstExpr,
    high: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    Ok(DataFusionExpr::Between(Between::new(
        Box::new(bind_expr_with_subqueries(
            expr,
            planner_context,
            subquery_planner,
        )?),
        negated,
        Box::new(bind_expr_with_subqueries(
            low,
            planner_context,
            subquery_planner,
        )?),
        Box::new(bind_expr_with_subqueries(
            high,
            planner_context,
            subquery_planner,
        )?),
    )))
}

pub(super) fn bind_in_list(
    expr: &AstExpr,
    list: &[AstExpr],
    negated: bool,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    Ok(DataFusionExpr::InList(InList::new(
        Box::new(bind_expr_with_subqueries(
            expr,
            planner_context,
            subquery_planner,
        )?),
        list.iter()
            .map(|expr| bind_expr_with_subqueries(expr, planner_context, subquery_planner))
            .collect::<Result<Vec<_>, _>>()?,
        negated,
    )))
}
