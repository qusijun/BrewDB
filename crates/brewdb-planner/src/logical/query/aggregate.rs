use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::expr::QueryGroupBy;
use crate::planner::PlannerError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Column;
use datafusion_expr::expr::Sort as DataFusionSort;
use datafusion_expr::utils::expr_as_column_expr;
use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};

use super::select::QueryExpression;

pub(super) fn needs_aggregate(query: &QueryExpression) -> bool {
    match &query.group_by {
        QueryGroupBy::All | QueryGroupBy::Expressions(_) => true,
        QueryGroupBy::None => query
            .projection
            .iter()
            .chain(query.having.iter())
            .any(expr_contains_aggregate),
    }
}

pub(super) fn aggregate_exprs(query: &QueryExpression) -> Vec<DataFusionExpr> {
    let mut aggregates = Vec::new();
    for expr in query
        .projection
        .iter()
        .chain(query.having.iter())
        .chain(query.order_by.iter().map(|sort| &sort.expr))
    {
        collect_aggregate_exprs(expr, &mut aggregates);
    }
    aggregates
}

pub(super) fn group_by_all_exprs(query: &QueryExpression) -> Vec<DataFusionExpr> {
    query
        .projection
        .iter()
        .filter(|expr| !expr_contains_aggregate(expr))
        .cloned()
        .collect()
}

pub(super) fn aggregate_order_by_exprs(
    query: &QueryExpression,
    aggregate_projection_exprs: &[DataFusionExpr],
    input: &DataFusionLogicalPlan,
) -> Result<Vec<DataFusionSort>, PlannerError> {
    query
        .order_by
        .iter()
        .filter(|sort| expr_contains_aggregate(&sort.expr))
        .map(|sort| {
            Ok(DataFusionSort::new(
                rebase_expr(&sort.expr, aggregate_projection_exprs, input)?,
                sort.asc,
                sort.nulls_first,
            ))
        })
        .collect()
}

fn collect_aggregate_exprs(expr: &DataFusionExpr, aggregates: &mut Vec<DataFusionExpr>) {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    let _ = expr.apply(|node| {
        if matches!(
            node,
            DataFusionExpr::ScalarSubquery(_)
                | DataFusionExpr::InSubquery(_)
                | DataFusionExpr::Exists(_)
        ) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if matches!(node, DataFusionExpr::AggregateFunction(_)) && !aggregates.contains(node) {
            aggregates.push(node.clone());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

pub(super) fn expr_contains_aggregate(expr: &DataFusionExpr) -> bool {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut found = false;
    let _ = expr.apply(|node| {
        if matches!(
            node,
            DataFusionExpr::ScalarSubquery(_)
                | DataFusionExpr::InSubquery(_)
                | DataFusionExpr::Exists(_)
        ) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if matches!(node, DataFusionExpr::AggregateFunction(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

pub(super) fn aggregate_projection_exprs(
    plan: &DataFusionLogicalPlan,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    let DataFusionLogicalPlan::Aggregate(aggregate) = plan else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected aggregate plan, got `{plan:?}`"),
        });
    };
    Ok(aggregate
        .group_expr
        .iter()
        .chain(aggregate.aggr_expr.iter())
        .cloned()
        .collect())
}

pub(super) fn rebase_expr(
    expr: &DataFusionExpr,
    base_exprs: &[DataFusionExpr],
    plan: &DataFusionLogicalPlan,
) -> Result<DataFusionExpr, PlannerError> {
    use datafusion_common::tree_node::TreeNodeRecursion;

    expr.clone()
        .transform_down(|nested_expr| {
            if matches!(
                nested_expr,
                DataFusionExpr::ScalarSubquery(_)
                    | DataFusionExpr::InSubquery(_)
                    | DataFusionExpr::Exists(_)
            ) {
                return Ok(Transformed::new(
                    nested_expr,
                    false,
                    TreeNodeRecursion::Jump,
                ));
            }
            if let Some(base_expr) = matching_base_expr(&nested_expr, base_exprs) {
                rebase_column_expr(base_expr, plan).map(Transformed::yes)
            } else if matches!(nested_expr, DataFusionExpr::AggregateFunction(_)) {
                Ok(Transformed::new(
                    nested_expr,
                    false,
                    TreeNodeRecursion::Jump,
                ))
            } else {
                Ok(Transformed::no(nested_expr))
            }
        })
        .data()
        .map_err(map_df_plan_error)
}

fn matching_base_expr<'a>(
    expr: &DataFusionExpr,
    base_exprs: &'a [DataFusionExpr],
) -> Option<&'a DataFusionExpr> {
    base_exprs
        .iter()
        .find(|base_expr| *base_expr == expr)
        .or_else(|| {
            base_exprs.iter().find(|base_expr| {
                !matches!(base_expr, DataFusionExpr::Column(_))
                    && !matches!(expr, DataFusionExpr::Column(_))
                    && normalize_expr_name(&base_expr.schema_name().to_string())
                        == normalize_expr_name(&expr.schema_name().to_string())
            })
        })
}

fn rebase_column_expr(
    base_expr: &DataFusionExpr,
    plan: &DataFusionLogicalPlan,
) -> datafusion_common::Result<DataFusionExpr> {
    if matches!(base_expr, DataFusionExpr::AggregateFunction(_)) {
        return Ok(DataFusionExpr::Column(Column::from_name(
            base_expr.schema_name().to_string(),
        )));
    }
    expr_as_column_expr(base_expr, plan)
}

fn normalize_expr_name(name: &str) -> String {
    let mut normalized = String::new();
    let mut token_start = 0;
    for ch in name.chars() {
        if ch == '"' {
            continue;
        }
        if ch == '.' {
            normalized.truncate(token_start);
            token_start = normalized.len();
            continue;
        }
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            token_start = normalized.len() + ch.len_utf8();
        }
        normalized.push(ch);
    }
    normalized
}
