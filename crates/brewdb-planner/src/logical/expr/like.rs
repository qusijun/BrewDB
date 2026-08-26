use crate::parser::ast::{Expr as AstExpr, Value};
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::{Expr as DataFusionExpr, Like};

use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_like_expr(
    negated: bool,
    any: bool,
    expr: &AstExpr,
    pattern: &AstExpr,
    escape_char: Option<&crate::parser::ast::ValueWithSpan>,
    case_insensitive: bool,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    if any {
        return Err(PlannerError::UnsupportedPlan {
            reason: "ANY in LIKE expression is not supported yet".to_string(),
        });
    }
    let escape_char = match escape_char.map(|value| &value.value) {
        Some(Value::SingleQuotedString(value)) if value.chars().count() == 1 => {
            value.chars().next()
        }
        Some(value) => {
            return Err(PlannerError::InvalidPlan {
                reason: format!(
                    "LIKE escape character must be a single quoted character, got `{value}`"
                ),
            });
        }
        None => None,
    };
    Ok(DataFusionExpr::Like(Like::new(
        negated,
        Box::new(bind_expr_with_subqueries(
            expr,
            planner_context,
            subquery_planner,
        )?),
        Box::new(bind_expr_with_subqueries(
            pattern,
            planner_context,
            subquery_planner,
        )?),
        escape_char,
        case_insensitive,
    )))
}
