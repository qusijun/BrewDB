use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{
    Query, SelectItem, SelectItemQualifiedWildcardKind, Statement as AstStatement,
};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::expr::bind_projection_with_subqueries;
use crate::planner::logical::query::{plan_query_statement_with_outer, QueryBindScope};
use crate::planner::PlannerError;
use datafusion_common::{Column, TableReference};
use datafusion_expr::Expr as DataFusionExpr;

use super::scope::{
    rewrite_columns_to_visible_fields, rewrite_outer_references, QueryBindScopeExt,
};

pub(super) fn bind_projection_for_query(
    projection: &[SelectItem],
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    if projection
        .iter()
        .any(|item| matches!(item, SelectItem::QualifiedWildcard(_, _)))
    {
        return bind_projection_items_for_query(projection, tables, planner_context, scope);
    }
    let function_registry = planner_context.function_registry();
    let child_outer_scope = scope.child_outer_scope();
    let mut subquery_planner = |subquery: &Query| {
        plan_query_statement_with_outer(
            AstStatement::Query(Box::new(subquery.clone())),
            tables.to_vec(),
            function_registry,
            child_outer_scope.clone(),
        )
    };
    bind_projection_with_subqueries(projection, planner_context, &mut subquery_planner)?
        .into_iter()
        .map(|expr| rewrite_columns_to_visible_fields(expr, scope))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|expr| rewrite_outer_references(expr, scope))
        .collect()
}

fn bind_projection_items_for_query(
    projection: &[SelectItem],
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    let mut exprs = Vec::new();
    for item in projection {
        match item {
            SelectItem::QualifiedWildcard(kind, _) => {
                exprs.extend(bind_qualified_wildcard_for_query(kind, scope)?);
            }
            item => {
                exprs.extend(bind_projection_for_query(
                    std::slice::from_ref(item),
                    tables,
                    planner_context,
                    scope,
                )?);
            }
        }
    }
    Ok(exprs)
}

fn bind_qualified_wildcard_for_query(
    kind: &SelectItemQualifiedWildcardKind,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    let qualifier = match kind {
        SelectItemQualifiedWildcardKind::ObjectName(name) => name.to_string(),
        SelectItemQualifiedWildcardKind::Expr(expr) => {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported qualified wildcard expression `{expr}`"),
            });
        }
    };
    let exprs = scope
        .local
        .iter()
        .filter(|field| {
            field
                .qualifier
                .as_deref()
                .is_some_and(|field_qualifier| field_qualifier.eq_ignore_ascii_case(&qualifier))
        })
        .map(|field| {
            DataFusionExpr::Column(Column::new(
                field.qualifier.clone().map(TableReference::from),
                field.name.clone(),
            ))
        })
        .collect::<Vec<_>>();
    if exprs.is_empty() {
        return Err(PlannerError::InvalidPlan {
            reason: format!("qualified wildcard `{qualifier}.*` did not match any table"),
        });
    }
    Ok(exprs)
}
