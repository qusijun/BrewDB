use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{GroupByExpr, Query, Statement as AstStatement};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::expr::{bind_group_by_with_subqueries, QueryGroupBy};
use crate::planner::logical::query::{plan_query_statement_with_outer, QueryBindScope};
use crate::planner::PlannerError;

use super::scope::{
    rewrite_columns_to_visible_fields, rewrite_outer_references, QueryBindScopeExt,
};

pub(super) fn bind_group_by_for_query(
    group_by: &GroupByExpr,
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<QueryGroupBy, PlannerError> {
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
    match bind_group_by_with_subqueries(group_by, planner_context, &mut subquery_planner)? {
        QueryGroupBy::Expressions(expressions) => Ok(QueryGroupBy::Expressions(
            expressions
                .into_iter()
                .map(|expr| rewrite_columns_to_visible_fields(expr, scope))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|expr| rewrite_outer_references(expr, scope))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        group_by => Ok(group_by),
    }
}
