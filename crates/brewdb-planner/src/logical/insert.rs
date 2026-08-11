use std::sync::Arc;

use crate::errors::PlannerError;
use brewdb_sql_parser::ast::{SetExpr, Statement as AstStatement};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::logical_plan::dml::{DmlStatement, InsertOp, WriteOp};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder, TableSource, col};

use crate::errors::{map_common_error, map_df_plan_error};
use crate::logical::expr::bind_expr;

pub(crate) fn plan_insert_statement(
    ast: AstStatement,
    target_table: brewdb_catalog::TableCatalogEntry,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let input = build_insert_input(&ast, &target_table, function_registry)?;
    let table_name = TableReference::full(
        target_table.path.catalog(),
        target_table.path.database(),
        target_table.path.table(),
    );
    let target: Arc<dyn TableSource> = Arc::new(target_table);
    Ok(DataFusionLogicalPlan::Dml(DmlStatement::new(
        table_name,
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(input),
    )))
}

fn build_insert_input(
    ast: &AstStatement,
    target_table: &brewdb_catalog::TableCatalogEntry,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let AstStatement::Insert(insert) = ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected insert statement, got `{ast}`"),
        });
    };
    let Some(source) = insert.source.as_deref() else {
        return Err(PlannerError::InvalidPlan {
            reason: "INSERT statement requires an input source".to_owned(),
        });
    };
    match source.body.as_ref() {
        SetExpr::Values(values) => build_values_input(ast, target_table, values, function_registry),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported insert source `{other}`"),
        }),
    }
}

fn build_values_input(
    ast: &AstStatement,
    target_table: &brewdb_catalog::TableCatalogEntry,
    values: &brewdb_sql_parser::ast::Values,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    validate_insert_columns(ast, target_table)?;
    let rows = values
        .rows
        .iter()
        .map(|row| {
            row.content
                .iter()
                .map(|expr| bind_expr(expr, function_registry))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(
        DFSchema::try_from(
            target_table
                .table_schema
                .to_arrow_schema()
                .map_err(map_common_error)?,
        )
        .map_err(map_df_plan_error)?,
    );
    let values_plan = LogicalPlanBuilder::values_with_schema(rows, &schema)
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)?;
    let projection = target_table
        .table_schema
        .fields
        .iter()
        .enumerate()
        .map(|(idx, field)| col(format!("column{}", idx + 1)).alias(field.name.clone()))
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(values_plan)
        .project(projection)
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)
}

fn validate_insert_columns(
    ast: &AstStatement,
    target_table: &brewdb_catalog::TableCatalogEntry,
) -> Result<(), PlannerError> {
    let AstStatement::Insert(insert) = ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected insert statement, got `{ast}`"),
        });
    };
    if insert.columns.is_empty() {
        return Ok(());
    }
    let target_columns = target_table
        .table_schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    let insert_columns = insert
        .columns
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    if insert_columns.len() == target_columns.len()
        && insert_columns
            .iter()
            .zip(target_columns.iter())
            .all(|(actual, expected)| actual.eq_ignore_ascii_case(expected))
    {
        return Ok(());
    }
    Err(PlannerError::UnsupportedPlan {
        reason: "INSERT VALUES currently requires either no column list or the full target column list in table order".to_string(),
    })
}
