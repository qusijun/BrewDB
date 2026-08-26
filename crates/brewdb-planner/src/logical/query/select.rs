use crate::parser::ast::{Query, Select, SetExpr, Statement as AstStatement};
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::expr::{projection_is_passthrough_wildcard, QueryGroupBy};
use crate::planner::logical::query::{QueryBindScope, VisibleField};
use crate::planner::PlannerError;
use datafusion_expr::expr::Sort as DataFusionSort;
use datafusion_expr::{
    Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
};

use super::aggregate::{
    aggregate_exprs, aggregate_order_by_exprs, aggregate_projection_exprs, expr_contains_aggregate,
    group_by_all_exprs, needs_aggregate, rebase_expr,
};
use super::alias::{
    aliases_without_local_field_conflicts, extract_aliases, resolve_aliases_to_exprs,
};
use super::expression::bind_expr_for_query;
use super::group_by::bind_group_by_for_query;
use super::input::build_from_input;
use super::modifier::{
    bind_distinct, bind_limit, bind_order_by, resolve_group_by_positions, resolve_sort_positions,
    QueryDistinct, QueryLimit,
};
use super::projection::bind_projection_for_query;
use super::scope::visible_fields_for_select;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct QueryExpression {
    pub(super) distinct: QueryDistinct,
    pub(super) projection: Vec<DataFusionExpr>,
    pub(super) selection: Option<DataFusionExpr>,
    pub(super) group_by: QueryGroupBy,
    pub(super) having: Option<DataFusionExpr>,
    pub(super) order_by: Vec<DataFusionSort>,
    pub(super) limit: Option<QueryLimit>,
}

pub(super) fn plan_select_query(
    query: &Query,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let query_expr = bind_select_query(query, planner_context, outer_scope)?;
    build_query_input(
        &AstStatement::Query(Box::new(query.clone())),
        planner_context,
        &query_expr,
    )
}

fn build_query_input(
    ast: &AstStatement,
    planner_context: &QueryPlannerContext<'_>,
    query: &QueryExpression,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_from_input(ast, planner_context)?;
    if let Some(predicate) = &query.selection {
        input = LogicalPlanBuilder::from(input)
            .filter(predicate.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if needs_aggregate(query) {
        if matches!(query.distinct, QueryDistinct::On(_)) {
            return Err(PlannerError::UnsupportedPlan {
                reason: "DISTINCT ON expressions with GROUP BY or aggregation are not supported"
                    .to_owned(),
            });
        }
        let aggregates = aggregate_exprs(query);
        let group_keys = match &query.group_by {
            QueryGroupBy::Expressions(expressions) => {
                resolve_group_by_positions(expressions, &query.projection)?
            }
            QueryGroupBy::All => group_by_all_exprs(query),
            QueryGroupBy::None => Vec::new(),
        };
        input = LogicalPlanBuilder::from(input)
            .aggregate(group_keys, aggregates)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
        let aggregate_projection_exprs = aggregate_projection_exprs(&input)?;
        if let Some(predicate) = &query.having {
            let predicate = rebase_expr(predicate, &aggregate_projection_exprs, &input)?;
            input = LogicalPlanBuilder::from(input)
                .filter(predicate)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
        let aggregate_order_by =
            aggregate_order_by_exprs(query, &aggregate_projection_exprs, &input)?;
        if !aggregate_order_by.is_empty() {
            input = LogicalPlanBuilder::from(input)
                .sort(aggregate_order_by)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
        let projection = query
            .projection
            .iter()
            .map(|expr| rebase_expr(expr, &aggregate_projection_exprs, &input))
            .collect::<Result<Vec<_>, _>>()?;
        input = LogicalPlanBuilder::from(input)
            .project(projection)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    } else {
        if let Some(predicate) = &query.having {
            return Err(PlannerError::InvalidPlan {
                reason: format!(
                    "HAVING clause references: {predicate} must appear in the GROUP BY clause or be used in an aggregate function"
                ),
            });
        }
        // TODO: add DataFusion-style window function planning before final projection.
        if let QueryDistinct::On(on_expr) = &query.distinct {
            input = LogicalPlanBuilder::from(input)
                .distinct_on(on_expr.clone(), query.projection.clone(), None)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        } else if !projection_is_passthrough_wildcard(&query.projection) {
            input = LogicalPlanBuilder::from(input)
                .project(query.projection.clone())
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
    }
    match &query.distinct {
        QueryDistinct::All => {
            input = LogicalPlanBuilder::from(input)
                .distinct()
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
        QueryDistinct::None | QueryDistinct::On(_) => {}
    }
    let post_projection_order_by = query
        .order_by
        .iter()
        .filter(|sort| !expr_contains_aggregate(&sort.expr))
        .cloned()
        .collect::<Vec<_>>();
    if !post_projection_order_by.is_empty() {
        let order_by = resolve_sort_positions(&post_projection_order_by, &query.projection)?;
        input = LogicalPlanBuilder::from(input)
            .sort(order_by)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if let Some(limit) = &query.limit {
        input = LogicalPlanBuilder::from(input)
            .limit_by_expr(limit.skip.clone(), limit.fetch.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    Ok(input)
}

fn bind_select_query(
    query: &Query,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
) -> Result<QueryExpression, PlannerError> {
    match query.body.as_ref() {
        SetExpr::Select(select) => bind_select(query, select, planner_context, outer_scope),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

fn bind_select(
    query: &Query,
    select: &Select,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
) -> Result<QueryExpression, PlannerError> {
    if select.prewhere.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        // TODO: align these Select extensions with DataFusion's full select pipeline.
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported select shape `{select}`"),
        });
    }
    let tables = planner_context.tables();
    let scope = QueryBindScope {
        local: visible_fields_for_select(select, planner_context)?,
        outer: outer_scope,
    };
    let projection =
        bind_projection_for_query(&select.projection, tables, planner_context, &scope)?;
    let aliases = extract_aliases(&projection);
    let group_by_aliases = aliases_without_local_field_conflicts(&aliases, &scope);
    let group_by = match bind_group_by_for_query(&select.group_by, tables, planner_context, &scope)?
    {
        QueryGroupBy::Expressions(expressions) => QueryGroupBy::Expressions(
            expressions
                .into_iter()
                .map(|expr| resolve_aliases_to_exprs(expr, &group_by_aliases))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        group_by => group_by,
    };
    let having = select
        .having
        .as_ref()
        .map(|expr| bind_expr_for_query(expr, tables, planner_context, &scope))
        .transpose()?
        .map(|expr| resolve_aliases_to_exprs(expr, &aliases))
        .transpose()?;
    Ok(QueryExpression {
        distinct: bind_distinct(&select.distinct, tables, planner_context, &scope)?,
        order_by: bind_order_by(
            query.order_by.as_ref(),
            &projection,
            tables,
            planner_context,
            &scope,
        )?,
        limit: bind_limit(query.limit_clause.as_ref(), tables, planner_context, &scope)?,
        projection,
        selection: select
            .selection
            .as_ref()
            .map(|expr| bind_expr_for_query(expr, tables, planner_context, &scope))
            .transpose()?,
        group_by,
        having,
    })
}
