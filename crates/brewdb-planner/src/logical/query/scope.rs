use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{Select, TableAlias, TableFactor, TableWithJoins};
use crate::planner::errors::{map_common_error, map_df_plan_error};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::query::{QueryBindScope, VisibleField};
use crate::planner::PlannerError;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, TableReference};
use datafusion_expr::Expr as DataFusionExpr;

pub(super) fn rewrite_columns_to_visible_fields(
    expr: DataFusionExpr,
    scope: &QueryBindScope,
) -> Result<DataFusionExpr, PlannerError> {
    expr.transform_up(|nested_expr| match nested_expr {
        DataFusionExpr::Column(column) => {
            if let Some(field) = scope.resolve_local(&column) {
                return Ok(Transformed::yes(DataFusionExpr::Column(Column::new(
                    canonical_column_relation(&column, field),
                    field.name.clone(),
                ))));
            }
            Ok(Transformed::no(DataFusionExpr::Column(column)))
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
    .map_err(map_df_plan_error)
}

pub(super) trait QueryBindScopeExt {
    fn child_outer_scope(&self) -> Vec<VisibleField>;
    fn resolve_local(&self, column: &Column) -> Option<&VisibleField>;
    fn matches_local(&self, column: &Column) -> bool;
    fn resolve_outer(&self, column: &Column) -> Option<&VisibleField>;
}

impl QueryBindScopeExt for QueryBindScope {
    fn child_outer_scope(&self) -> Vec<VisibleField> {
        self.local
            .iter()
            .cloned()
            .chain(self.outer.iter().cloned())
            .collect()
    }

    fn resolve_local(&self, column: &Column) -> Option<&VisibleField> {
        let mut matches = self.local.iter().filter(|field| field.matches(column));
        let field = matches.next()?;
        matches.next().is_none().then_some(field)
    }

    fn matches_local(&self, column: &Column) -> bool {
        self.local.iter().any(|field| field.matches(column))
    }

    fn resolve_outer(&self, column: &Column) -> Option<&VisibleField> {
        let mut matches = self.outer.iter().filter(|field| field.matches(column));
        let field = matches.next()?;
        matches.next().is_none().then_some(field)
    }
}

pub(super) fn rewrite_outer_references(
    expr: DataFusionExpr,
    scope: &QueryBindScope,
) -> Result<DataFusionExpr, PlannerError> {
    expr.transform_up(|nested_expr| match nested_expr {
        DataFusionExpr::Column(column) => {
            if scope.matches_local(&column) {
                return Ok(Transformed::no(DataFusionExpr::Column(column)));
            }
            if let Some(field) = scope.resolve_outer(&column) {
                return Ok(Transformed::yes(DataFusionExpr::OuterReferenceColumn(
                    field.field.clone(),
                    Column::new(column.relation.clone(), column.name.clone()),
                )));
            }
            Ok(Transformed::no(DataFusionExpr::Column(column)))
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
    .map_err(map_df_plan_error)
}

trait VisibleFieldExt {
    fn matches(&self, column: &Column) -> bool;
}

impl VisibleFieldExt for VisibleField {
    fn matches(&self, column: &Column) -> bool {
        if !self.name.eq_ignore_ascii_case(&column.name) {
            return false;
        }
        let Some(relation) = &column.relation else {
            return true;
        };
        self.qualifier.as_deref().is_some_and(|qualifier| {
            qualifier.eq_ignore_ascii_case(relation.table())
                || qualifier.eq_ignore_ascii_case(&relation.to_string())
        })
    }
}

fn canonical_column_relation(column: &Column, field: &VisibleField) -> Option<TableReference> {
    column.relation.as_ref().map(|relation| {
        field
            .qualifier
            .as_deref()
            .map(TableReference::bare)
            .unwrap_or_else(|| relation.clone())
    })
}

pub(super) fn visible_fields_for_select(
    select: &Select,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<Vec<VisibleField>, PlannerError> {
    let mut fields = Vec::new();
    for from in &select.from {
        collect_visible_fields_for_table_with_joins(from, planner_context, &mut fields)?;
    }
    Ok(fields)
}

fn collect_visible_fields_for_table_with_joins(
    from: &TableWithJoins,
    planner_context: &QueryPlannerContext<'_>,
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    collect_visible_fields_for_table_factor(&from.relation, planner_context, fields)?;
    for join in &from.joins {
        collect_visible_fields_for_table_factor(&join.relation, planner_context, fields)?;
    }
    Ok(())
}

fn collect_visible_fields_for_table_factor(
    factor: &TableFactor,
    planner_context: &QueryPlannerContext<'_>,
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            if let Some(cte_plan) = planner_context.cte(name)? {
                let qualifier = alias_name(alias).unwrap_or_else(|| name.to_string());
                return append_plan_visible_fields(&cte_plan, Some(qualifier), fields);
            }
            let table = planner_context.resolve_table(name)?;
            let qualifier = alias_name(alias).unwrap_or_else(|| name.to_string());
            append_table_visible_fields(&table, Some(qualifier), fields)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_visible_fields_for_table_with_joins(table_with_joins, planner_context, fields),
        TableFactor::Derived { alias, .. } => {
            if let Some(alias) = alias {
                for column in &alias.columns {
                    fields.push(VisibleField {
                        qualifier: Some(alias.name.value.clone()),
                        name: column.name.value.clone(),
                        field: Arc::new(arrow::datatypes::Field::new(
                            column.name.value.clone(),
                            arrow::datatypes::DataType::Null,
                            true,
                        )),
                    });
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn append_table_visible_fields(
    table: &TableCatalogEntry,
    qualifier: Option<String>,
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    for field in &table.table_schema.fields {
        fields.push(VisibleField {
            qualifier: qualifier.clone(),
            name: field.name.clone(),
            field: Arc::new(field.to_arrow_field().map_err(map_common_error)?),
        });
    }
    Ok(())
}

fn append_plan_visible_fields(
    plan: &datafusion_expr::LogicalPlan,
    qualifier: Option<String>,
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    fields.extend(plan.schema().iter().map(|(_, field)| VisibleField {
        qualifier: qualifier.clone(),
        name: field.name().clone(),
        field: field.clone(),
    }));
    Ok(())
}

pub(in crate::logical) fn visible_fields_for_tables(
    tables: &[TableCatalogEntry],
) -> Result<Vec<VisibleField>, PlannerError> {
    let mut fields = Vec::new();
    for table in tables {
        append_table_visible_fields(table, Some(table.path.table().to_owned()), &mut fields)?;
    }
    Ok(fields)
}

fn alias_name(alias: &Option<TableAlias>) -> Option<String> {
    alias.as_ref().map(|alias| alias.name.value.clone())
}
