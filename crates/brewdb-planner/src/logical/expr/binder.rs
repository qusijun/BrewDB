use crate::parser::ast::Expr as AstExpr;
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};

use super::case::bind_case;
use super::identifier::{bind_compound_identifier, bind_identifier};
use super::interval::bind_interval;
use super::like::bind_like_expr;
use super::operator::{bind_binary_expr, bind_unary_expr};
use super::predicate::{bind_between, bind_in_list, bind_is_null};
use super::subquery::unsupported_subquery_planner;
use super::subquery::{bind_exists, bind_in_subquery, bind_scalar_subquery};
use super::{extract, function, substring, value};

pub(in crate::logical) type SubqueryPlanner<'a> =
    dyn FnMut(&crate::parser::ast::Query) -> Result<DataFusionLogicalPlan, PlannerError> + 'a;

pub(in crate::logical) fn bind_expr_with_context(
    expr: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    bind_expr_with_subqueries(expr, planner_context, &mut unsupported_subquery_planner)
}

pub(in crate::logical) fn bind_expr_with_subqueries(
    expr: &AstExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        AstExpr::Identifier(ident) => Ok(bind_identifier(ident)),
        AstExpr::CompoundIdentifier(idents) => Ok(bind_compound_identifier(idents)),
        AstExpr::Value(value) => value::bind_value(value),
        AstExpr::Nested(expr) => bind_expr_with_subqueries(expr, planner_context, subquery_planner),
        AstExpr::BinaryOp { left, op, right } => {
            bind_binary_expr(left, op, right, planner_context, subquery_planner)
        }
        AstExpr::UnaryOp { op, expr } => {
            bind_unary_expr(op, expr, planner_context, subquery_planner)
        }
        AstExpr::IsNull(expr) => bind_is_null(expr, false, planner_context, subquery_planner),
        AstExpr::IsNotNull(expr) => bind_is_null(expr, true, planner_context, subquery_planner),
        AstExpr::Between {
            expr,
            negated,
            low,
            high,
        } => bind_between(expr, *negated, low, high, planner_context, subquery_planner),
        AstExpr::InList {
            expr,
            list,
            negated,
        } => bind_in_list(expr, list, *negated, planner_context, subquery_planner),
        AstExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => bind_in_subquery(expr, subquery, *negated, planner_context, subquery_planner),
        AstExpr::Exists { subquery, negated } => bind_exists(subquery, *negated, subquery_planner),
        AstExpr::Subquery(subquery) => bind_scalar_subquery(subquery, subquery_planner),
        AstExpr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            false,
            planner_context,
            subquery_planner,
        ),
        AstExpr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            true,
            planner_context,
            subquery_planner,
        ),
        AstExpr::TypedString(typed) => value::bind_typed_string(typed),
        AstExpr::Interval(interval) => bind_interval(interval),
        AstExpr::Extract { field, expr, .. } => {
            extract::bind_extract(field, expr, planner_context, subquery_planner)
        }
        AstExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => substring::bind_substring(
            expr,
            substring_from,
            substring_for,
            planner_context,
            subquery_planner,
        ),
        AstExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => bind_case(
            operand.as_deref(),
            conditions,
            else_result.as_deref(),
            planner_context,
            subquery_planner,
        ),
        AstExpr::Function(function) => {
            function::bind_function(function, planner_context, subquery_planner)
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported expression `{other}`"),
        }),
    }
}
