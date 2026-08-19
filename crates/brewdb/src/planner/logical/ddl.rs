use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
use crate::parser::ast::{
    AlterTable, ColumnDef, ColumnOption, CreateTable, CreateTableOptions, DataType as AstDataType,
    ExactNumberInfo, Expr, Ident, ObjectName, PrimaryKeyConstraint, ShowStatementOptions,
    SqlOption, TableConstraint, ValueWithSpan, WrappedCollection,
};
use crate::SqlError;
use datafusion_common::{Constraint, Constraints, DFSchema, TableReference};
use datafusion_expr::{
    CreateExternalTable, DdlStatement, DropTable, LogicalPlan as DataFusionLogicalPlan,
};

use crate::planner::logical::plan::{CreateDatabase, Ddl, DropDatabase, LogicalPlanNode, Show};
use crate::planner::logical::{
    empty_df_schema, extension_plan, name_parts, object_name_to_string, qualify_database_name,
    qualify_table_name, resolve_table, LogicalPlanningContext, LogicalPlanningSession,
};

pub(crate) fn bind_create_database_statement(
    session: LogicalPlanningSession,
    db_name: &ObjectName,
) -> Result<DataFusionLogicalPlan, SqlError> {
    Ok(extension_plan(LogicalPlanNode::Ddl(Ddl::CreateDatabase(
        CreateDatabase {
            catalog_name: session.catalog_name,
            database_name: db_name.to_string(),
        },
    ))))
}

pub(crate) fn bind_create_table_statement(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    create_table: &CreateTable,
) -> Result<DataFusionLogicalPlan, SqlError> {
    if create_table.columns.is_empty() {
        return Err(SqlError::InvalidRequest {
            reason: "CREATE TABLE must define at least one column".to_string(),
        });
    }
    let (catalog_name, database_name, table_name) =
        qualify_table_name(session, &create_table.name)?;
    let catalog = ctx
        .catalog_service
        .open_catalog(&catalog_name)
        .map_err(|error| SqlError::InvalidRequest {
            reason: error.to_string(),
        })?;
    let file_type = catalog.entry().storage_kind.as_str();
    let mut table_options = create_table_options(&create_table.table_options);
    let partition_keys = bind_table_partitioning(create_table)?;
    let distribution = bind_table_distribution(create_table)?;
    let primary_keys = bind_primary_keys(create_table)?;
    let cluster_keys = bind_table_clustering(create_table)?;
    let table_location = create_table
        .location
        .clone()
        .or_else(|| table_options.remove("location"));
    let mut table_schema = TableSchema::new(
        create_table
            .columns
            .iter()
            .map(bind_column_def)
            .collect::<Result<Vec<_>, SqlError>>()?,
    )
    .with_primary_keys(primary_keys.clone())
    .with_partition_keys(partition_keys.clone())
    .with_bucket_keys(distribution.bucket_keys.clone())
    .with_cluster_keys(cluster_keys);
    table_schema.bucket_count = distribution.bucket_count;
    table_schema.bucket_function = distribution.bucket_function;
    let schema = table_schema
        .to_arrow_schema()
        .map_err(|error| SqlError::InvalidRequest {
            reason: error.to_string(),
        })
        .and_then(|schema| {
            DFSchema::try_from(schema).map_err(|error| SqlError::InvalidRequest {
                reason: error.to_string(),
            })
        })?;
    let constraints = primary_key_constraints(&table_schema, &primary_keys)?;
    let table_location = table_location.unwrap_or_default();
    Ok(DataFusionLogicalPlan::Ddl(
        DdlStatement::CreateExternalTable(
            CreateExternalTable::builder(
                TableReference::full(catalog_name, database_name, table_name),
                table_location,
                file_type,
                Arc::new(schema),
            )
            .with_partition_cols(partition_keys)
            .with_options(table_options.into_iter().collect())
            .with_constraints(constraints)
            .build(),
        ),
    ))
}

pub(crate) fn bind_drop_statement(
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    object_type: &crate::parser::ast::ObjectType,
    if_exists: bool,
    names: &[ObjectName],
) -> Result<DataFusionLogicalPlan, SqlError> {
    let Some(name) = names.first() else {
        return Err(SqlError::InvalidRequest {
            reason: "DROP statement must carry at least one object name".to_string(),
        });
    };

    match object_type {
        crate::parser::ast::ObjectType::Table => {
            let table = match resolve_table(ctx, session, name) {
                Ok(table) => table,
                Err(error) => {
                    if if_exists
                        && matches!(
                        &error,
                        SqlError::InvalidRequest { reason }
                            if reason.contains("table not found")
                        )
                    {
                        return Ok(DataFusionLogicalPlan::EmptyRelation(
                            datafusion_expr::EmptyRelation {
                                produce_one_row: false,
                                schema: empty_df_schema(),
                            },
                        ));
                    }
                    return Err(error);
                }
            };
            Ok(DataFusionLogicalPlan::Ddl(DdlStatement::DropTable(
                DropTable {
                    name: TableReference::full(
                        table.path.catalog(),
                        table.path.database(),
                        table.path.table(),
                    ),
                    if_exists,
                    schema: empty_df_schema(),
                },
            )))
        }
        crate::parser::ast::ObjectType::Database => {
            let (catalog_name, database_name) = qualify_database_name(session, name)?;
            Ok(extension_plan(LogicalPlanNode::Ddl(Ddl::DropDatabase(
                DropDatabase {
                    catalog_name,
                    database_name,
                },
            ))))
        }
        _ => Err(SqlError::UnsupportedStatement {
            reason: format!("DROP {object_type}"),
        }),
    }
}

pub(crate) fn bind_alter_statement(
    session: LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    alter_table: &AlterTable,
) -> Result<DataFusionLogicalPlan, SqlError> {
    let _ = (session, ctx, alter_table);
    Err(SqlError::UnsupportedStatement {
        reason: "ALTER TABLE is not supported by DataFusion DDL yet".to_string(),
    })
}

pub(crate) fn bind_show_catalogs_statement() -> Result<DataFusionLogicalPlan, SqlError> {
    Ok(show_plan(Show::Catalogs))
}

pub(crate) fn bind_show_databases_statement(
    session: LogicalPlanningSession,
    show_options: &ShowStatementOptions,
) -> Result<DataFusionLogicalPlan, SqlError> {
    let catalog_name = show_options
        .show_in
        .as_ref()
        .and_then(|show_in| show_in.parent_name.as_ref())
        .map(object_name_to_string)
        .unwrap_or(session.catalog_name);
    Ok(show_plan(Show::Databases { catalog_name }))
}

pub(crate) fn bind_show_tables_statement(
    session: LogicalPlanningSession,
    show_options: &ShowStatementOptions,
) -> Result<DataFusionLogicalPlan, SqlError> {
    let (catalog_name, database_name) = show_options
        .show_in
        .as_ref()
        .and_then(|show_in| show_in.parent_name.as_ref())
        .map(|name| {
            let parts = name_parts(name)?;
            match parts.as_slice() {
                [database] => Ok((session.catalog_name.clone(), database.clone())),
                [catalog, database] => Ok((catalog.clone(), database.clone())),
                _ => Err(SqlError::InvalidRequest {
                    reason: format!("invalid SHOW TABLES target `{name}`"),
                }),
            }
        })
        .transpose()?
        .unwrap_or((session.catalog_name, session.database_name));

    Ok(show_plan(Show::Tables {
        catalog_name,
        database_name,
    }))
}

pub(crate) fn bind_show_variable_statement(
    variable: &[Ident],
) -> Result<DataFusionLogicalPlan, SqlError> {
    Err(SqlError::UnsupportedStatement {
        reason: format!("unsupported SHOW statement `SHOW {}`", ident_list(variable)),
    })
}

fn create_table_options(options: &CreateTableOptions) -> BTreeMap<String, String> {
    let entries = match options {
        CreateTableOptions::None => &[][..],
        CreateTableOptions::With(entries)
        | CreateTableOptions::Options(entries)
        | CreateTableOptions::Plain(entries)
        | CreateTableOptions::TableProperties(entries) => entries.as_slice(),
    };
    sql_options(entries)
}

fn sql_options(options: &[SqlOption]) -> BTreeMap<String, String> {
    options
        .iter()
        .filter_map(sql_option_entry)
        .collect::<BTreeMap<_, _>>()
}

fn sql_option_entry(option: &SqlOption) -> Option<(String, String)> {
    match option {
        SqlOption::KeyValue { key, value } => Some((key.value.clone(), expr_to_string(value))),
        SqlOption::Ident(ident) => Some((ident.value.clone(), "true".to_string())),
        _ => None,
    }
}

fn bind_column_def(column: &ColumnDef) -> Result<ColumnField, SqlError> {
    let mut planned = ColumnField::new(
        column.name.value.clone(),
        bind_data_type(&column.data_type)?,
    );
    planned.nullable = !column.options.iter().any(|option| {
        matches!(
            option.option,
            ColumnOption::NotNull | ColumnOption::PrimaryKey(_)
        )
    });
    Ok(planned)
}

fn bind_primary_keys(create_table: &CreateTable) -> Result<Vec<String>, SqlError> {
    let mut primary_keys = Vec::new();
    for column in &create_table.columns {
        for option in &column.options {
            if matches!(option.option, ColumnOption::PrimaryKey(_)) {
                if !primary_keys.is_empty() {
                    return Err(SqlError::InvalidRequest {
                        reason: "CREATE TABLE must not define multiple PRIMARY KEY constraints"
                            .to_string(),
                    });
                }
                primary_keys.push(column.name.value.clone());
            }
        }
    }

    for constraint in &create_table.constraints {
        if let TableConstraint::PrimaryKey(PrimaryKeyConstraint { columns, .. }) = constraint {
            if !primary_keys.is_empty() {
                return Err(SqlError::InvalidRequest {
                    reason: "CREATE TABLE must not define multiple PRIMARY KEY constraints"
                        .to_string(),
                });
            }
            primary_keys = columns
                .iter()
                .map(primary_key_column_name)
                .collect::<Result<Vec<_>, _>>()?;
        }
    }

    validate_primary_keys(&create_table.columns, &primary_keys)?;
    Ok(primary_keys)
}

fn primary_key_column_name(column: &crate::parser::ast::IndexColumn) -> Result<String, SqlError> {
    match &column.column.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
        other => Err(SqlError::InvalidRequest {
            reason: format!("PRIMARY KEY column must be a simple column name, got `{other}`"),
        }),
    }
}

fn validate_primary_keys(columns: &[ColumnDef], primary_keys: &[String]) -> Result<(), SqlError> {
    if primary_keys.is_empty() {
        return Ok(());
    }

    let mut seen = BTreeSet::new();
    for key in primary_keys {
        if !seen.insert(key.clone()) {
            return Err(SqlError::InvalidRequest {
                reason: format!("PRIMARY KEY must not contain duplicate column `{key}`"),
            });
        }
    }

    let column_names = columns
        .iter()
        .map(|column| column.name.value.as_str())
        .collect::<BTreeSet<_>>();
    for key in primary_keys {
        if !column_names.contains(key.as_str()) {
            return Err(SqlError::InvalidRequest {
                reason: format!("PRIMARY KEY column `{key}` is not defined in CREATE TABLE"),
            });
        }
    }
    Ok(())
}

fn primary_key_constraints(
    schema: &TableSchema,
    primary_keys: &[String],
) -> Result<Constraints, SqlError> {
    if primary_keys.is_empty() {
        return Ok(Constraints::default());
    }

    let mut indices = Vec::with_capacity(primary_keys.len());
    for primary_key in primary_keys {
        let Some(index) = schema
            .fields
            .iter()
            .position(|field| field.name == *primary_key)
        else {
            return Err(SqlError::InvalidRequest {
                reason: format!(
                    "PRIMARY KEY column `{primary_key}` is not defined in CREATE TABLE"
                ),
            });
        };
        indices.push(index);
    }
    Ok(Constraints::new_unverified(vec![Constraint::PrimaryKey(
        indices,
    )]))
}

fn bind_table_partitioning(create_table: &CreateTable) -> Result<Vec<String>, SqlError> {
    if create_table.partitioned_by.is_empty() {
        return Ok(Vec::new());
    }

    let partition_columns = create_table
        .partitioned_by
        .iter()
        .map(|column| column.value.clone())
        .collect::<Vec<_>>();
    validate_column_names(
        &create_table.columns,
        &partition_columns,
        "PARTITIONED BY",
        "partition",
    )?;
    Ok(partition_columns)
}

fn bind_table_clustering(create_table: &CreateTable) -> Result<Vec<String>, SqlError> {
    let Some(cluster_by) = &create_table.cluster_by else {
        return Ok(Vec::new());
    };

    let cluster_columns = match cluster_by {
        WrappedCollection::NoWrapping(exprs) | WrappedCollection::Parentheses(exprs) => exprs
            .iter()
            .map(cluster_column_name)
            .collect::<Result<Vec<_>, _>>()?,
    };
    validate_column_names(
        &create_table.columns,
        &cluster_columns,
        "CLUSTER BY",
        "cluster",
    )?;
    Ok(cluster_columns)
}

fn cluster_column_name(expr: &Expr) -> Result<String, SqlError> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
        other => Err(SqlError::InvalidRequest {
            reason: format!("CLUSTER BY column must be a simple column name, got `{other}`"),
        }),
    }
}

fn validate_column_names(
    columns: &[ColumnDef],
    names: &[String],
    clause_name: &str,
    column_role: &str,
) -> Result<(), SqlError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name.clone()) {
            return Err(SqlError::InvalidRequest {
                reason: format!("{clause_name} must not contain duplicate column `{name}`"),
            });
        }
    }

    let column_names = columns
        .iter()
        .map(|column| column.name.value.as_str())
        .collect::<BTreeSet<_>>();
    for name in names {
        if !column_names.contains(name.as_str()) {
            return Err(SqlError::InvalidRequest {
                reason: format!(
                    "{clause_name} {column_role} column `{name}` is not defined in CREATE TABLE"
                ),
            });
        }
    }
    Ok(())
}

#[derive(Default)]
struct TableDistribution {
    bucket_keys: Vec<String>,
    bucket_count: Option<u32>,
    bucket_function: Option<String>,
}

fn bind_table_distribution(create_table: &CreateTable) -> Result<TableDistribution, SqlError> {
    let Some(distribution) = &create_table.distributed_by else {
        return Ok(TableDistribution::default());
    };
    if distribution.columns.is_empty() && distribution.function.is_some() {
        return Err(SqlError::InvalidRequest {
            reason: "bucket function requires at least one bucket key".to_string(),
        });
    }

    let bucket_keys = distribution
        .columns
        .iter()
        .map(|column| column.value.clone())
        .collect::<Vec<_>>();
    validate_column_names(
        &create_table.columns,
        &bucket_keys,
        "DISTRIBUTED BY",
        "bucket",
    )?;
    let bucket_count = distribution
        .buckets
        .map(|bucket_count| {
            u32::try_from(bucket_count).map_err(|_| SqlError::InvalidRequest {
                reason: format!("bucket count `{bucket_count}` is too large"),
            })
        })
        .transpose()?;
    Ok(TableDistribution {
        bucket_keys,
        bucket_count,
        bucket_function: distribution
            .function
            .as_ref()
            .map(|function| function.value.clone()),
    })
}

fn bind_data_type(data_type: &AstDataType) -> Result<DataType, SqlError> {
    match data_type {
        AstDataType::Boolean => Ok(DataType::Boolean),
        AstDataType::TinyInt(_) => Ok(DataType::Int8),
        AstDataType::SmallInt(_) => Ok(DataType::Int16),
        AstDataType::Int(_) | AstDataType::Integer(_) => Ok(DataType::Int32),
        AstDataType::BigInt(_) => Ok(DataType::Int64),
        AstDataType::Float(_) | AstDataType::Real => Ok(DataType::Float32),
        AstDataType::Double(_) | AstDataType::DoublePrecision => Ok(DataType::Double),
        AstDataType::Binary(_) | AstDataType::Varbinary(_) | AstDataType::Blob(_) => {
            Ok(DataType::Binary)
        }
        AstDataType::Text
        | AstDataType::String(_)
        | AstDataType::Varchar(_)
        | AstDataType::Char(_)
        | AstDataType::Character(_) => Ok(DataType::String),
        AstDataType::Date => Ok(DataType::Date),
        AstDataType::Time(_, timezone) => Ok(DataType::Time {
            precision: timezone_precision(timezone, 0),
        }),
        AstDataType::Timestamp(_, timezone) => Ok(DataType::Timestamp {
            precision: timezone_precision(timezone, 6),
            with_time_zone: !matches!(
                timezone,
                crate::parser::ast::TimezoneInfo::None
                    | crate::parser::ast::TimezoneInfo::WithoutTimeZone
            ),
        }),
        AstDataType::Datetime(precision) => Ok(DataType::Timestamp {
            precision: precision.map_or(6, |value| value as u32),
            with_time_zone: false,
        }),
        AstDataType::Decimal(info) | AstDataType::Numeric(info) | AstDataType::Dec(info) => {
            let (precision, scale) = decimal_precision_scale(info);
            Ok(DataType::Decimal { precision, scale })
        }
        other => Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported data type `{other}`"),
        }),
    }
}

fn timezone_precision(timezone: &crate::parser::ast::TimezoneInfo, default_precision: u32) -> u32 {
    let _ = timezone;
    default_precision
}

fn decimal_precision_scale(info: &ExactNumberInfo) -> (u32, u32) {
    match info {
        ExactNumberInfo::None => (38, 0),
        ExactNumberInfo::Precision(precision) => (*precision as u32, 0),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => {
            (*precision as u32, (*scale).max(0) as u32)
        }
    }
}

fn show_plan(show: Show) -> DataFusionLogicalPlan {
    extension_plan(LogicalPlanNode::Show(show))
}

fn ident_list(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => value.to_string(),
        other => other.to_string(),
    }
}
