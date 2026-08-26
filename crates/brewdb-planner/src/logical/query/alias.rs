use std::collections::HashMap;

use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::query::QueryBindScope;
use crate::planner::PlannerError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::Expr as DataFusionExpr;

pub(super) fn extract_aliases(exprs: &[DataFusionExpr]) -> HashMap<String, DataFusionExpr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            DataFusionExpr::Alias(alias) => {
                Some((alias.name.to_ascii_lowercase(), *alias.expr.clone()))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn aliases_without_local_field_conflicts(
    aliases: &HashMap<String, DataFusionExpr>,
    scope: &QueryBindScope,
) -> HashMap<String, DataFusionExpr> {
    let mut aliases = aliases.clone();
    for field in &scope.local {
        aliases.remove(&field.name.to_ascii_lowercase());
    }
    aliases
}

pub(super) fn resolve_aliases_to_exprs(
    expr: DataFusionExpr,
    aliases: &HashMap<String, DataFusionExpr>,
) -> Result<DataFusionExpr, PlannerError> {
    expr.transform_up(|nested_expr| match nested_expr {
        DataFusionExpr::Column(column) if column.relation.is_none() => {
            if let Some(alias_expr) = aliases.get(&column.name.to_ascii_lowercase()) {
                Ok(Transformed::yes(alias_expr.clone()))
            } else {
                Ok(Transformed::no(DataFusionExpr::Column(column)))
            }
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
    .map_err(map_df_plan_error)
}
