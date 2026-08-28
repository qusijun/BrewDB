use crate::parser::ast::Expr as AstExpr;
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_common::Spans;
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, Subquery as DataFusionSubquery};
use std::sync::Arc;

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn unsupported_subquery_planner(
    subquery: &crate::parser::ast::Query,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    Err(PlannerError::UnsupportedPlan {
        reason: format!("unsupported subquery `{subquery}`"),
    })
}

fn bind_subquery(
    subquery: &crate::parser::ast::Query,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionSubquery, PlannerError> {
    let subquery = subquery_planner(subquery)?;
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Ok(DataFusionSubquery {
        subquery: Arc::new(subquery),
        outer_ref_columns,
        spans: Spans::new(),
    })
}

pub(super) fn validate_single_column_subquery(
    subquery: &DataFusionSubquery,
) -> Result<(), PlannerError> {
    let column_count = subquery.subquery.schema().fields().len();
    if column_count == 1 {
        return Ok(());
    }
    Err(PlannerError::Plan {
        reason: format!("subquery must return exactly one column, got {column_count}"),
        cause: None,
    })
}

pub(super) fn bind_in_subquery(
    expr: &AstExpr,
    subquery: &crate::parser::ast::Query,
    negated: bool,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<datafusion_expr::Expr, PlannerError> {
    let subquery = bind_subquery(subquery, subquery_planner)?;
    validate_single_column_subquery(&subquery)?;
    Ok(datafusion_expr::Expr::InSubquery(InSubquery::new(
        Box::new(bind_expr_with_subqueries(
            expr,
            planner_context,
            subquery_planner,
        )?),
        subquery,
        negated,
    )))
}

pub(super) fn bind_exists(
    subquery: &crate::parser::ast::Query,
    negated: bool,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<datafusion_expr::Expr, PlannerError> {
    Ok(datafusion_expr::Expr::Exists(Exists::new(
        bind_subquery(subquery, subquery_planner)?,
        negated,
    )))
}

pub(super) fn bind_scalar_subquery(
    subquery: &crate::parser::ast::Query,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<datafusion_expr::Expr, PlannerError> {
    let subquery = bind_subquery(subquery, subquery_planner)?;
    validate_single_column_subquery(&subquery)?;
    Ok(datafusion_expr::Expr::ScalarSubquery(subquery))
}
