use crate::parser::ast::{GroupByExpr, SelectItem, SelectItemQualifiedWildcardKind};
use crate::planner::errors::PlannerError;
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::expr::WildcardOptions;
use datafusion_expr::Expr as DataFusionExpr;

use super::{bind_expr_with_subqueries, SubqueryPlanner};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::logical) enum QueryGroupBy {
    None,
    All,
    Expressions(Vec<DataFusionExpr>),
}

pub(in crate::logical) fn bind_group_by_with_subqueries(
    group_by: &GroupByExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<QueryGroupBy, PlannerError> {
    match group_by {
        GroupByExpr::All(_) => Ok(QueryGroupBy::All),
        GroupByExpr::Expressions(expressions, _) if expressions.is_empty() => {
            Ok(QueryGroupBy::None)
        }
        GroupByExpr::Expressions(expressions, _) => Ok(QueryGroupBy::Expressions(
            expressions
                .iter()
                .map(|expr| bind_expr_with_subqueries(expr, planner_context, subquery_planner))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

pub(in crate::logical) fn bind_projection_with_subqueries(
    items: &[SelectItem],
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    items
        .iter()
        .map(|item| bind_select_item(item, planner_context, subquery_planner))
        .collect()
}

fn bind_select_item(
    item: &SelectItem,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            bind_expr_with_subqueries(expr, planner_context, subquery_planner)
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            Ok(
                bind_expr_with_subqueries(expr, planner_context, subquery_planner)?
                    .alias(alias.value.clone()),
            )
        }
        SelectItem::ExprWithAliases { expr, aliases } => {
            let Some(alias) = aliases.first() else {
                return Err(PlannerError::InvalidPlan {
                    reason: "projection aliases must not be empty".to_string(),
                });
            };
            Ok(
                bind_expr_with_subqueries(expr, planner_context, subquery_planner)?
                    .alias(alias.value.clone()),
            )
        }
        SelectItem::Wildcard(_) => Ok(wildcard_expr()),
        SelectItem::QualifiedWildcard(kind, _) => bind_qualified_wildcard(kind),
    }
}

#[allow(deprecated)]
pub(super) fn bind_qualified_wildcard(
    kind: &SelectItemQualifiedWildcardKind,
) -> Result<DataFusionExpr, PlannerError> {
    match kind {
        SelectItemQualifiedWildcardKind::ObjectName(name) => Ok(DataFusionExpr::Wildcard {
            qualifier: Some(name.to_string().into()),
            options: Box::new(WildcardOptions::default()),
        }),
        SelectItemQualifiedWildcardKind::Expr(expr) => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported qualified wildcard expression `{expr}`"),
        }),
    }
}

#[allow(deprecated)]
pub(in crate::logical) fn projection_is_passthrough_wildcard(
    projection: &[DataFusionExpr],
) -> bool {
    matches!(
        projection,
        [DataFusionExpr::Wildcard {
            qualifier: None,
            ..
        }]
    )
}

#[allow(deprecated)]
pub(super) fn wildcard_expr() -> DataFusionExpr {
    DataFusionExpr::Wildcard {
        qualifier: None,
        options: Box::default(),
    }
}
