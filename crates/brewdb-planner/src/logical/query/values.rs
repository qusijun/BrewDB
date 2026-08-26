use crate::parser::ast::Values;
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::expr::bind_expr_with_context;
use crate::planner::PlannerError;
use datafusion_common::DFSchema;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;

pub(super) fn plan_values(
    values: &Values,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    if values.value_keyword {
        return Err(PlannerError::UnsupportedPlan {
            reason: "`VALUE` keyword not supported. Did you mean `VALUES`?".to_owned(),
        });
    }
    build_values_plan(values, planner_context, None)
}

pub(crate) fn build_values_plan(
    values: &Values,
    planner_context: &QueryPlannerContext<'_>,
    schema: Option<Arc<DFSchema>>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expr| bind_expr_with_context(expr, planner_context))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    match schema {
        Some(schema) if !schema.fields().is_empty() => {
            LogicalPlanBuilder::values_with_schema(rows, &schema)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        _ => LogicalPlanBuilder::values(rows)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error),
    }
}
