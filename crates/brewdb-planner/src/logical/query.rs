use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{Query, SetExpr, Statement as AstStatement};
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::{
    resolve_query_tables, LogicalPlanningContext, LogicalPlanningSession,
};
use crate::planner::PlannerError;
use arrow::datatypes::FieldRef;
use datafusion_common::Column;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
};

mod aggregate;
mod alias;
mod cte;
mod expression;
mod group_by;
mod input;
mod modifier;
mod projection;
mod scope;
mod select;
mod set_expr;
pub(super) mod values;

pub(super) use expression::bind_expr_for_query;
use modifier::{bind_limit, bind_order_by, resolve_sort_positions};
pub(super) use scope::visible_fields_for_tables;
use select::plan_select_query;

#[derive(Clone, Debug, Default)]
pub(super) struct QueryBindScope {
    pub(super) local: Vec<VisibleField>,
    pub(super) outer: Vec<VisibleField>,
}

#[derive(Clone, Debug)]
pub(super) struct VisibleField {
    pub(super) qualifier: Option<String>,
    pub(super) name: String,
    pub(super) field: FieldRef,
}

pub(crate) fn plan_query_statement(
    ast: AstStatement,
    tables: Vec<TableCatalogEntry>,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    plan_query_statement_with_outer(ast, tables, function_registry, Vec::new())
}

pub(super) fn plan_query_statement_with_outer(
    ast: AstStatement,
    tables: Vec<TableCatalogEntry>,
    function_registry: &dyn FunctionRegistry,
    outer_scope: Vec<VisibleField>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let planner_context = QueryPlannerContext::new(&tables, function_registry);
    let AstStatement::Query(query) = ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{ast}`"),
        });
    };
    // TODO: align DataFusion query-level extensions such as FETCH and pipe operators if needed.
    plan_query(&query, &planner_context, outer_scope)
}

pub(crate) fn bind_query_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    query: &Query,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let tables = resolve_query_tables(ctx, session, query)?;
    let plan = plan_query_statement(ast, tables.clone(), function_registry)?;
    let _ = tables;
    Ok(plan)
}

pub(super) fn plan_query(
    query: &Query,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let cte_names = query
        .with
        .as_ref()
        .map(|with| cte::plan_with_clause(with, planner_context, outer_scope.clone()))
        .transpose()?
        .unwrap_or_default();
    let plan_result = (|| match query.body.as_ref() {
        SetExpr::Select(_) => plan_select_query(query, planner_context, outer_scope),
        body => {
            let mut plan_nested_query =
                |query: &Query, outer_scope| plan_query(query, planner_context, outer_scope);
            let mut plan = set_expr::plan_set_expr(
                body,
                planner_context,
                outer_scope.clone(),
                &mut plan_nested_query,
            )?;
            let scope = scope_for_plan_output(&plan, outer_scope);
            let projection = projection_for_plan_output(&plan);
            let order_by = bind_order_by(
                query.order_by.as_ref(),
                &projection,
                planner_context.tables(),
                planner_context,
                &scope,
            )?;
            if !order_by.is_empty() {
                let order_by = resolve_sort_positions(&order_by, &projection)?;
                plan = LogicalPlanBuilder::from(plan)
                    .sort(order_by)
                    .map_err(map_df_plan_error)?
                    .build()
                    .map_err(map_df_plan_error)?;
            }
            if let Some(limit) = bind_limit(
                query.limit_clause.as_ref(),
                planner_context.tables(),
                planner_context,
                &scope,
            )? {
                plan = LogicalPlanBuilder::from(plan)
                    .limit_by_expr(limit.skip, limit.fetch)
                    .map_err(map_df_plan_error)?
                    .build()
                    .map_err(map_df_plan_error)?;
            }
            Ok(plan)
        }
    })();
    for cte_name in cte_names {
        planner_context.remove_cte(&cte_name);
    }
    plan_result
}

fn scope_for_plan_output(
    plan: &DataFusionLogicalPlan,
    outer_scope: Vec<VisibleField>,
) -> QueryBindScope {
    QueryBindScope {
        local: plan
            .schema()
            .iter()
            .map(|(qualifier, field)| VisibleField {
                qualifier: qualifier.as_ref().map(ToString::to_string),
                name: field.name().clone(),
                field: field.clone(),
            })
            .collect(),
        outer: outer_scope,
    }
}

fn projection_for_plan_output(plan: &DataFusionLogicalPlan) -> Vec<DataFusionExpr> {
    plan.schema()
        .iter()
        .map(|(qualifier, field)| {
            DataFusionExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
        })
        .collect()
}
