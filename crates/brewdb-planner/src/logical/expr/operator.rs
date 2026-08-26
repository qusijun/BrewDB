use crate::parser::ast::{
    BinaryOperator as AstBinaryOperator, Expr as AstExpr, UnaryOperator as AstUnaryOperator,
};
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::{BinaryExpr, Expr as DataFusionExpr, Operator as DataFusionOperator};

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_unary_expr(
    op: &AstUnaryOperator,
    expr: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let expr = bind_expr_with_subqueries(expr, planner_context, subquery_planner)?;
    match op {
        AstUnaryOperator::Not => Ok(DataFusionExpr::Not(Box::new(expr))),
        AstUnaryOperator::Minus => Ok(DataFusionExpr::Negative(Box::new(expr))),
        AstUnaryOperator::Plus => Ok(expr),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported unary operator `{other}`"),
        }),
    }
}

pub(super) fn bind_binary_expr(
    left: &AstExpr,
    op: &AstBinaryOperator,
    right: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    Ok(DataFusionExpr::BinaryExpr(BinaryExpr::new(
        Box::new(bind_expr_with_subqueries(
            left,
            planner_context,
            subquery_planner,
        )?),
        bind_binary_operator(op)?,
        Box::new(bind_expr_with_subqueries(
            right,
            planner_context,
            subquery_planner,
        )?),
    )))
}

fn bind_binary_operator(op: &AstBinaryOperator) -> Result<DataFusionOperator, PlannerError> {
    match op {
        AstBinaryOperator::Plus => Ok(DataFusionOperator::Plus),
        AstBinaryOperator::Minus => Ok(DataFusionOperator::Minus),
        AstBinaryOperator::Multiply => Ok(DataFusionOperator::Multiply),
        AstBinaryOperator::Divide => Ok(DataFusionOperator::Divide),
        AstBinaryOperator::Modulo => Ok(DataFusionOperator::Modulo),
        AstBinaryOperator::StringConcat => Ok(DataFusionOperator::StringConcat),
        AstBinaryOperator::Gt => Ok(DataFusionOperator::Gt),
        AstBinaryOperator::Lt => Ok(DataFusionOperator::Lt),
        AstBinaryOperator::GtEq => Ok(DataFusionOperator::GtEq),
        AstBinaryOperator::LtEq => Ok(DataFusionOperator::LtEq),
        AstBinaryOperator::Eq => Ok(DataFusionOperator::Eq),
        AstBinaryOperator::NotEq => Ok(DataFusionOperator::NotEq),
        AstBinaryOperator::And => Ok(DataFusionOperator::And),
        AstBinaryOperator::Or => Ok(DataFusionOperator::Or),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported binary operator `{other}`"),
        }),
    }
}
