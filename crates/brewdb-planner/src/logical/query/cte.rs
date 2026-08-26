use crate::parser::ast::With;
use crate::planner::logical::context::{cte_name_from_ident, QueryPlannerContext};
use crate::planner::logical::relation::apply_table_alias;
use crate::planner::PlannerError;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;

use super::{plan_query, VisibleField};

pub(super) fn plan_with_clause(
    with: &With,
    planner_context: &QueryPlannerContext<'_>,
    outer_scope: Vec<VisibleField>,
) -> Result<Vec<String>, PlannerError> {
    if with.recursive {
        // TODO: align recursive CTE planning with DataFusion when BrewDB needs it.
        return Err(PlannerError::UnsupportedPlan {
            reason: "recursive CTEs are not supported yet".to_owned(),
        });
    }
    let mut cte_names = Vec::with_capacity(with.cte_tables.len());
    for cte in &with.cte_tables {
        if cte.from.is_some() {
            // TODO: support extended CTE shapes once they are needed by workloads.
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported CTE shape `{cte}`"),
            });
        }
        let cte_name = cte_name_from_ident(&cte.alias.name);
        if planner_context.contains_cte(&cte_name) {
            return Err(PlannerError::InvalidPlan {
                reason: format!("WITH query name `{cte_name}` specified more than once"),
            });
        }
        let plan = plan_query(&cte.query, planner_context, outer_scope.clone())?;
        planner_context.insert_cte(cte_name.clone(), apply_cte_alias(plan, &cte.alias)?);
        cte_names.push(cte_name);
    }
    Ok(cte_names)
}

fn apply_cte_alias(
    plan: DataFusionLogicalPlan,
    alias: &crate::parser::ast::TableAlias,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    apply_table_alias(plan, alias).map_err(|error| match error {
        PlannerError::InvalidPlan { reason } => PlannerError::InvalidPlan {
            reason: reason.replace("derived table alias", "CTE alias"),
        },
        other => other,
    })
}
