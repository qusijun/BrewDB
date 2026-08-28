use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{Distinct as AstDistinct, LimitClause, OrderBy, OrderByKind};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::query::QueryBindScope;
use crate::planner::PlannerError;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::Sort as DataFusionSort;
use datafusion_expr::Expr as DataFusionExpr;

use super::bind_expr_for_query;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct QueryLimit {
    pub(super) skip: Option<DataFusionExpr>,
    pub(super) fetch: Option<DataFusionExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum QueryDistinct {
    None,
    All,
    On(Vec<DataFusionExpr>),
}

pub(super) fn bind_distinct(
    distinct: &Option<AstDistinct>,
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<QueryDistinct, PlannerError> {
    match distinct {
        None | Some(AstDistinct::All) => Ok(QueryDistinct::None),
        Some(AstDistinct::Distinct) => Ok(QueryDistinct::All),
        Some(AstDistinct::On(exprs)) => exprs
            .iter()
            .map(|expr| bind_expr_for_query(expr, tables, planner_context, scope))
            .collect::<Result<Vec<_>, _>>()
            .map(QueryDistinct::On),
    }
}

pub(super) fn bind_order_by(
    order_by: Option<&OrderBy>,
    projection: &[DataFusionExpr],
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionSort>, PlannerError> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported order by shape `{order_by}`"),
        });
    }
    let expressions = match &order_by.kind {
        OrderByKind::Expressions(expressions) => expressions
            .iter()
            .map(|expr| {
                if expr.with_fill.is_some() {
                    return Err(PlannerError::UnsupportedPlan {
                        reason: format!("unsupported order by expression `{expr}`"),
                    });
                }
                Ok::<_, PlannerError>(DataFusionSort::new(
                    bind_expr_for_query(&expr.expr, tables, planner_context, scope)?,
                    expr.options.asc.unwrap_or(true),
                    nulls_first_or_datafusion_default(expr.options.asc, expr.options.nulls_first),
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?,
        OrderByKind::All(options) => projection
            .iter()
            .map(|expr| {
                Ok::<_, PlannerError>(DataFusionSort::new(
                    order_by_all_expr(expr)?,
                    options.asc.unwrap_or(true),
                    nulls_first_or_datafusion_default(options.asc, options.nulls_first),
                ))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?,
    };
    Ok(expressions)
}

pub(super) fn bind_limit(
    limit_clause: Option<&LimitClause>,
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<Option<QueryLimit>, PlannerError> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    let limit = match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("limit by is not supported yet: {limit_by:?}"),
                });
            }
            QueryLimit {
                skip: offset
                    .as_ref()
                    .map(|offset| {
                        bind_expr_for_query(&offset.value, tables, planner_context, scope)
                    })
                    .transpose()?,
                fetch: limit
                    .as_ref()
                    .map(|limit| bind_expr_for_query(limit, tables, planner_context, scope))
                    .transpose()?,
            }
        }
        LimitClause::OffsetCommaLimit { offset, limit } => QueryLimit {
            skip: Some(bind_expr_for_query(offset, tables, planner_context, scope)?),
            fetch: Some(bind_expr_for_query(limit, tables, planner_context, scope)?),
        },
    };
    Ok(Some(limit))
}

pub(super) fn resolve_group_by_positions(
    group_by: &[DataFusionExpr],
    projection: &[DataFusionExpr],
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    group_by
        .iter()
        .map(|expr| match expr {
            DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _)
                if *position > 0 && (*position as usize) <= projection.len() =>
            {
                Ok(unalias_expr(projection[*position as usize - 1].clone()))
            }
            DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _) => {
                Err(PlannerError::InvalidPlan {
                    reason: format!("GROUP BY position {position} is out of range"),
                })
            }
            _ => Ok(expr.clone()),
        })
        .collect()
}

fn unalias_expr(expr: DataFusionExpr) -> DataFusionExpr {
    match expr {
        DataFusionExpr::Alias(alias) => *alias.expr,
        _ => expr,
    }
}

pub(super) fn resolve_sort_positions(
    sort_exprs: &[DataFusionSort],
    projection: &[DataFusionExpr],
) -> Result<Vec<DataFusionSort>, PlannerError> {
    sort_exprs
        .iter()
        .map(|sort| {
            Ok(DataFusionSort::new(
                resolve_position_to_expr(sort.expr.clone(), projection)?,
                sort.asc,
                sort.nulls_first,
            ))
        })
        .collect()
}

fn resolve_position_to_expr(
    expr: DataFusionExpr,
    projection: &[DataFusionExpr],
) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _)
            if position > 0 && (position as usize) <= projection.len() =>
        {
            Ok(projected_column_expr(&projection[position as usize - 1]))
        }
        DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _) => {
            Err(PlannerError::InvalidPlan {
                reason: format!("ORDER BY position {position} is out of range"),
            })
        }
        DataFusionExpr::Column(column) if column.relation.is_none() => {
            Ok(resolve_projected_column(&column.name, projection)
                .unwrap_or(DataFusionExpr::Column(column)))
        }
        _ => Ok(expr),
    }
}

fn resolve_projected_column(name: &str, projection: &[DataFusionExpr]) -> Option<DataFusionExpr> {
    projection.iter().find_map(|expr| match expr {
        DataFusionExpr::Alias(alias) if alias.name.eq_ignore_ascii_case(name) => {
            Some(projected_column_expr(expr))
        }
        DataFusionExpr::Column(column) if column.name.eq_ignore_ascii_case(name) => {
            Some(projected_column_expr(expr))
        }
        _ => None,
    })
}

fn projected_column_expr(expr: &DataFusionExpr) -> DataFusionExpr {
    match expr {
        DataFusionExpr::Alias(alias) => {
            DataFusionExpr::Column(Column::from_name(alias.name.clone()))
        }
        DataFusionExpr::Column(column) => DataFusionExpr::Column(column.clone()),
        _ => DataFusionExpr::Column(Column::from_name(expr.schema_name().to_string())),
    }
}

fn order_by_all_expr(expr: &DataFusionExpr) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        DataFusionExpr::Alias(alias) => Ok(DataFusionExpr::Column(Column::from_name(
            alias.name.clone(),
        ))),
        DataFusionExpr::Column(column) => Ok(DataFusionExpr::Column(column.clone())),
        _ => Err(PlannerError::UnsupportedPlan {
            reason: format!("ORDER BY ALL is not supported for non-column expression `{expr}`"),
        }),
    }
}

fn nulls_first_or_datafusion_default(asc: Option<bool>, nulls_first: Option<bool>) -> bool {
    nulls_first.unwrap_or_else(|| !asc.unwrap_or(true))
}
