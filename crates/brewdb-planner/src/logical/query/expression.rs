use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{Query, Statement as AstStatement};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::expr::bind_expr_with_subqueries;
use crate::planner::logical::query::{plan_query_statement_with_outer, QueryBindScope};
use crate::planner::PlannerError;
use datafusion_expr::Expr as DataFusionExpr;

use super::scope::{
    rewrite_columns_to_visible_fields, rewrite_outer_references, QueryBindScopeExt,
};

pub(in crate::logical) fn bind_expr_for_query(
    expr: &crate::parser::ast::Expr,
    tables: &[TableCatalogEntry],
    planner_context: &QueryPlannerContext<'_>,
    scope: &QueryBindScope,
) -> Result<DataFusionExpr, PlannerError> {
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
    let expr = bind_expr_with_subqueries(expr, planner_context, &mut subquery_planner)?;
    let expr = rewrite_columns_to_visible_fields(expr, scope)?;
    rewrite_outer_references(expr, scope)
}
