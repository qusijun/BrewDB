use crate::parser::ast::{Query, SetExpr, SetOperator, SetQuantifier};
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::query::select::plan_select_query;
use crate::planner::logical::query::values::plan_values;
use crate::planner::logical::query::VisibleField;
use crate::planner::PlannerError;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder};

pub(super) fn plan_set_expr<F>(
    set_expr: &SetExpr,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
    plan_query: &mut F,
) -> Result<DataFusionLogicalPlan, PlannerError>
where
    F: FnMut(&Query, Vec<VisibleField>) -> Result<DataFusionLogicalPlan, PlannerError>,
{
    match set_expr {
        SetExpr::Select(_) => plan_select_query(
            &Query {
                with: None,
                body: Box::new(set_expr.clone()),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            },
            planner_context,
            outer_scope,
        ),
        SetExpr::Values(values) => plan_values(values, planner_context),
        SetExpr::SetOperation {
            left,
            op,
            set_quantifier,
            right,
        } => {
            let left = plan_set_expr(left, planner_context, outer_scope.clone(), plan_query)?;
            let right = plan_set_expr(right, planner_context, outer_scope, plan_query)?;
            set_operation_to_plan(*op, left, right, *set_quantifier)
        }
        SetExpr::Query(query) => plan_query(query, outer_scope),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

pub(super) fn set_operation_to_plan(
    op: SetOperator,
    left: DataFusionLogicalPlan,
    right: DataFusionLogicalPlan,
    quantifier: SetQuantifier,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    match (op, quantifier) {
        (SetOperator::Union, SetQuantifier::All) => LogicalPlanBuilder::from(left)
            .union(right)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error),
        (SetOperator::Union, SetQuantifier::Distinct | SetQuantifier::None) => {
            LogicalPlanBuilder::from(left)
                .union_distinct(right)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        (SetOperator::Intersect, SetQuantifier::All) => {
            LogicalPlanBuilder::intersect(left, right, true).map_err(map_df_plan_error)
        }
        (SetOperator::Intersect, SetQuantifier::Distinct | SetQuantifier::None) => {
            LogicalPlanBuilder::intersect(left, right, false).map_err(map_df_plan_error)
        }
        (SetOperator::Except, SetQuantifier::All) => {
            LogicalPlanBuilder::except(left, right, true).map_err(map_df_plan_error)
        }
        (SetOperator::Except, SetQuantifier::Distinct | SetQuantifier::None) => {
            LogicalPlanBuilder::except(left, right, false).map_err(map_df_plan_error)
        }
        (op, quantifier) => Err(PlannerError::UnsupportedPlan {
            reason: format!("{op} {quantifier} not implemented"),
        }),
    }
}
