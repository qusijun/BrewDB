use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::common::utils::normalize_file_path;
use crate::parser::ast::{
    CopyLegacyOption, CopyOption, CopySource, CopyTarget, Delete, Insert, Merge, SetExpr,
    Statement as AstStatement, Update,
};
use crate::planner::PlannerError;
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::logical_plan::dml::{DmlStatement, InsertOp, WriteOp};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{col, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder, TableSource};

use crate::planner::errors::{map_common_error, map_df_plan_error};
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::query::values::build_values_plan;
use crate::planner::logical::table_source::DefaultTableSource;
use crate::planner::logical::{
    resolve_query_tables, resolve_table, resolve_table_object, LogicalPlanningContext,
    LogicalPlanningSession,
};
use crate::storage::open_storage_engine;

pub(crate) fn bind_insert_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    insert: &Insert,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let target_table = resolve_table_object(ctx, session, &insert.table)?;
    let source_tables = insert
        .source
        .as_deref()
        .map(|query| resolve_query_tables(ctx, session, query))
        .transpose()?
        .unwrap_or_default();
    let plan = plan_insert_statement(ast, target_table.clone(), function_registry)?;
    let mut table_catalogs = Vec::with_capacity(source_tables.len() + 1);
    table_catalogs.push(target_table);
    table_catalogs.extend(source_tables);
    let _ = table_catalogs;
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn bind_copy_statement(
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    source: &CopySource,
    to: bool,
    target: &CopyTarget,
    options: &[CopyOption],
    legacy_options: &[CopyLegacyOption],
    values: &[Option<String>],
) -> Result<DataFusionLogicalPlan, PlannerError> {
    if to {
        return Err(PlannerError::UnsupportedPlan {
            reason: "COPY TO is not supported yet; use SELECT for reads and COPY FROM for imports"
                .to_string(),
        });
    }
    if !legacy_options.is_empty() || !values.is_empty() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "legacy COPY options and inline COPY payloads are not supported yet"
                .to_string(),
        });
    }
    let (filename, table_name, columns) = match (source, target) {
        (
            CopySource::File { filename },
            CopyTarget::Table {
                table_name,
                columns,
            },
        ) => (filename.as_str(), table_name, columns),
        (
            CopySource::Table {
                table_name,
                columns,
            },
            CopyTarget::File { filename },
        ) => (filename.as_str(), table_name, columns),
        _ => {
            return Err(PlannerError::UnsupportedPlan {
                reason: "COPY FROM supports a file source and table target".to_string(),
            });
        }
    };
    if !columns.is_empty() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "COPY FROM with an explicit column list is not supported yet".to_string(),
        });
    }

    let target_table = resolve_table(ctx, session, table_name)?;
    let copy_options = bind_copy_from_file_options(options)?;
    plan_copy_from_file_statement(target_table, filename, copy_options)
}

pub(crate) fn bind_delete_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    delete: &Delete,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let _ = (ast, session, ctx, delete);
    Err(PlannerError::UnsupportedPlan {
        reason: "DELETE is not supported yet".to_owned(),
    })
}

pub(crate) fn bind_update_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    update: &Update,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let _ = (ast, session, ctx, update);
    Err(PlannerError::UnsupportedPlan {
        reason: "UPDATE is not supported yet".to_owned(),
    })
}

pub(crate) fn bind_merge_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    merge: &Merge,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let _ = (ast, session, ctx, merge);
    Err(PlannerError::UnsupportedPlan {
        reason: "MERGE is not supported yet".to_owned(),
    })
}

pub(crate) fn plan_insert_statement(
    ast: AstStatement,
    target_table: crate::catalog::TableCatalogEntry,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let input = build_insert_input(&ast, &target_table, function_registry)?;
    let table_name = TableReference::full(
        target_table.path.catalog(),
        target_table.path.database(),
        target_table.path.table(),
    );
    let target = table_source_for_catalog(target_table)?;
    Ok(DataFusionLogicalPlan::Dml(DmlStatement::new(
        table_name,
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(input),
    )))
}

fn build_insert_input(
    ast: &AstStatement,
    target_table: &crate::catalog::TableCatalogEntry,
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
    target_table: &crate::catalog::TableCatalogEntry,
    values: &crate::parser::ast::Values,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    validate_insert_columns(ast, target_table)?;
    let tables = [];
    let planner_context = QueryPlannerContext::new(&tables, function_registry);
    let schema = Arc::new(
        DFSchema::try_from(
            target_table
                .table_schema
                .to_arrow_schema()
                .map_err(map_common_error)?,
        )
        .map_err(map_df_plan_error)?,
    );
    let values_plan = build_values_plan(values, &planner_context, Some(schema))?;
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
    target_table: &crate::catalog::TableCatalogEntry,
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

fn plan_copy_from_file_statement(
    target_table: TableCatalogEntry,
    filename: &str,
    file_options: BTreeMap<String, String>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    ensure_copy_from_single_file(filename)?;
    // COPY FROM is the file engine's expected-schema path. This mirrors
    // DuckDB's COPY FROM bind flow: the target table provides expected names
    // and types, and the file reader is responsible for reading/casting source
    // rows into that contract. Ordinary file scans still infer file schema.
    let table_name = TableReference::full(
        target_table.path.catalog(),
        target_table.path.database(),
        target_table.path.table(),
    );
    let target = table_source_for_catalog(target_table.clone())?;
    let source_name = normalize_file_path(filename);
    let source_table = TableCatalogEntry::temporary_file_with_schema(
        source_name.clone(),
        filename.to_owned(),
        target_table.table_schema.clone(),
        file_options,
    )
    .map_err(|error| PlannerError::InvalidPlan {
        reason: error.to_string(),
    })?;
    let source = table_source_for_catalog(source_table)?;
    let input = LogicalPlanBuilder::scan(source_name, source, None)
        .map_err(|error| PlannerError::InvalidPlan {
            reason: error.to_string(),
        })?
        .build()
        .map_err(|error| PlannerError::InvalidPlan {
            reason: error.to_string(),
        })?;
    Ok(DataFusionLogicalPlan::Dml(DmlStatement::new(
        table_name,
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(input),
    )))
}

fn table_source_for_catalog(
    table: TableCatalogEntry,
) -> Result<Arc<dyn TableSource>, PlannerError> {
    let table_engine = open_storage_engine()
        .and_then(|storage| storage.table_engine(&table))
        .map_err(|error| PlannerError::InvalidPlan {
            reason: error.to_string(),
        })?;
    Ok(Arc::new(DefaultTableSource::new(table, table_engine)))
}

fn ensure_copy_from_single_file(location: &str) -> Result<(), PlannerError> {
    let metadata = fs::metadata(location).map_err(|error| PlannerError::InvalidPlan {
        reason: error.to_string(),
    })?;
    if metadata.is_file() {
        return Ok(());
    }
    Err(PlannerError::InvalidPlan {
        reason: format!("COPY FROM expects a single file: {location}"),
    })
}

fn bind_copy_from_file_options(
    options: &[CopyOption],
) -> Result<BTreeMap<String, String>, PlannerError> {
    let mut file_options = BTreeMap::new();
    for option in options {
        match option {
            CopyOption::Format(format) => {
                file_options.insert("format".to_owned(), format.value.to_ascii_lowercase());
            }
            CopyOption::Header(has_header) => {
                file_options.insert("has_header".to_owned(), has_header.to_string());
            }
            other => {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported COPY FROM option `{other}`"),
                });
            }
        }
    }
    file_options
        .entry("has_header".to_owned())
        .or_insert_with(|| "false".to_owned());
    Ok(file_options)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry, TablePath};
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_common::test_util::{TestDir, TestFile};

    use super::plan_copy_from_file_statement;

    fn target_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "brewdb", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "s3://warehouse/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn copy_from_rejects_directory_source_before_file_engine_setup() {
        let dir = TestDir::new("brewdb-copy-from-dir");
        fs::write(dir.path().join("part-1.csv"), "id\n11\n").unwrap();

        let error = plan_copy_from_file_statement(
            target_table(),
            &dir.path().to_string_lossy(),
            [("has_header".to_owned(), "true".to_owned())].into(),
        )
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("COPY FROM expects a single file"));
    }

    #[test]
    fn copy_from_plans_source_scan_with_target_schema() {
        let file = TestFile::new("brewdb-copy-from-mismatch", "csv");
        fs::write(file.path(), "1,2\n").unwrap();

        let plan = plan_copy_from_file_statement(
            target_table(),
            &file.path().to_string_lossy(),
            BTreeMap::new(),
        )
        .unwrap();

        let datafusion_expr::LogicalPlan::Dml(dml) = plan else {
            panic!("expected COPY FROM to bind to a DML plan");
        };
        let datafusion_expr::LogicalPlan::TableScan(scan) = dml.input.as_ref() else {
            panic!("expected COPY FROM input to be a table scan");
        };
        assert_eq!(scan.source.schema().field(0).name(), "id");
        assert_eq!(
            scan.source.schema().field(0).data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }
}
