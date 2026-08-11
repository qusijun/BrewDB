//! SQL statement logical planner.

mod expr;
pub(crate) mod insert;
pub mod optimizer;
pub mod plan;
pub(crate) mod query;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use brewdb_catalog::{CatalogPath, CatalogService, TableCatalogEntry};
use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
use brewdb_sql_parser::ast::{
    AlterTable, AnalyzeFormatKind, ColumnDef, ColumnOption, CreateTable, CreateTableOptions,
    DataType as AstDataType, Delete, ExactNumberInfo, Expr, Ident, Insert, Merge, ObjectName,
    ObjectNamePart, PrimaryKeyConstraint, Query, Set, SetExpr, ShowStatementOptions, SqlOption,
    Statement as AstStatement, TableConstraint, TableFactor, TableObject, TransactionAccessMode,
    TransactionMode as AstTransactionMode, Update, Use, Value, ValueWithSpan,
};

use brewdb_sql::{SqlError, SqlRequestContext, SqlSessionContext, Statement};
use datafusion_common::display::{PlanType, ToStringifiedPlan};
use datafusion_common::{Constraint, Constraints, DFSchema, DFSchemaRef, TableReference};
use datafusion_expr::registry::MemoryFunctionRegistry;
use datafusion_expr::{
    CreateExternalTable, DdlStatement, DropTable, Explain, Extension,
    LogicalPlan as DataFusionLogicalPlan, SetVariable, Statement as DataFusionStatement,
    TransactionAccessMode as DataFusionTransactionAccessMode,
    TransactionConclusion as DataFusionTransactionConclusion,
    TransactionEnd as DataFusionTransactionEnd, TransactionIsolationLevel, TransactionStart,
};
use datafusion_functions as datafusion_scalar_functions;
use datafusion_functions_aggregate as datafusion_aggregate_functions;

use crate::errors::PlannerError;
use plan::{CreateDatabase, Ddl, DropDatabase, LogicalPlanNode, Show};

use self::insert::plan_insert_statement;
pub use self::optimizer::LogicalOptimizer;
use self::query::plan_query_statement;

pub struct LogicalPlanningContext<'a> {
    pub session: &'a SqlSessionContext,
    pub request: &'a SqlRequestContext,
    pub catalog_service: &'a CatalogService,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalPlanningSession {
    pub session_id: uuid::Uuid,
    pub user_name: String,
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Debug)]
pub struct LogicalPlanner {
    function_registry: MemoryFunctionRegistry,
}

impl Default for LogicalPlanner {
    fn default() -> Self {
        Self {
            function_registry: default_function_registry(),
        }
    }
}

impl LogicalPlanner {
    pub fn plan(
        &self,
        statement: Statement,
        ctx: &LogicalPlanningContext<'_>,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let planning_session = planning_session(ctx)?;
        let ast = statement.clone();
        match &ast {
            AstStatement::Query(query) => self.bind_query(statement, planning_session, ctx, query),
            AstStatement::Insert(insert) => {
                self.bind_insert(statement, planning_session, ctx, insert)
            }
            AstStatement::Delete(delete) => {
                self.bind_delete(statement, planning_session, ctx, delete)
            }
            AstStatement::Update(update) => {
                self.bind_update(statement, planning_session, ctx, update)
            }
            AstStatement::Merge(merge) => self.bind_merge(statement, planning_session, ctx, merge),
            AstStatement::CreateDatabase { db_name, .. } => Ok(extension_plan(
                LogicalPlanNode::Ddl(Ddl::CreateDatabase(CreateDatabase {
                    catalog_name: planning_session.catalog_name,
                    database_name: db_name.to_string(),
                })),
            )),
            AstStatement::CreateTable(create_table) => {
                self.bind_create_table(&planning_session, create_table)
            }
            AstStatement::Drop {
                object_type, names, ..
            } => self.bind_drop(&planning_session, ctx, object_type, names),
            AstStatement::AlterTable(alter_table) => {
                self.bind_alter(planning_session, ctx, alter_table)
            }
            AstStatement::ShowCatalogs { .. } => Ok(show_plan(Show::Catalogs)),
            AstStatement::ShowDatabases { show_options, .. }
            | AstStatement::ShowSchemas { show_options, .. } => {
                self.bind_show_databases(planning_session, show_options)
            }
            AstStatement::ShowTables { show_options, .. } => {
                self.bind_show_tables(planning_session, show_options)
            }
            AstStatement::ShowVariable { variable } => Err(SqlError::UnsupportedStatement {
                reason: format!("unsupported SHOW statement `SHOW {}`", ident_list(variable)),
            }),
            AstStatement::Set(set) => self.bind_set(set, planning_session),
            AstStatement::Use(use_stmt) => self.bind_use(planning_session, use_stmt),
            AstStatement::StartTransaction { modes, .. } => Ok(DataFusionLogicalPlan::Statement(
                DataFusionStatement::TransactionStart(TransactionStart {
                    access_mode: bind_txn_access_mode(modes),
                    isolation_level: TransactionIsolationLevel::ReadCommitted,
                }),
            )),
            AstStatement::Commit { .. } => Ok(DataFusionLogicalPlan::Statement(
                DataFusionStatement::TransactionEnd(DataFusionTransactionEnd {
                    conclusion: DataFusionTransactionConclusion::Commit,
                    chain: false,
                }),
            )),
            AstStatement::Rollback { .. } => Ok(DataFusionLogicalPlan::Statement(
                DataFusionStatement::TransactionEnd(DataFusionTransactionEnd {
                    conclusion: DataFusionTransactionConclusion::Rollback,
                    chain: false,
                }),
            )),
            AstStatement::Explain {
                statement, format, ..
            } => self.bind_explain(statement, *format, ctx),
            _ => Err(SqlError::UnsupportedStatement {
                reason: statement.to_string(),
            }),
        }
    }

    fn bind_query(
        &self,
        ast: AstStatement,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        query: &Query,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let tables = resolve_query_tables(ctx, &session, query)?;
        let plan = plan_query_statement(ast, tables.clone(), &self.function_registry)
            .map_err(planner_to_sql_error)?;
        let _ = tables;
        Ok(plan)
    }

    fn bind_insert(
        &self,
        ast: AstStatement,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        insert: &Insert,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let target_table = resolve_table_object(ctx, &session, &insert.table)?;
        let source_tables = insert
            .source
            .as_deref()
            .map(|query| resolve_query_tables(ctx, &session, query))
            .transpose()?
            .unwrap_or_default();
        let plan = plan_insert_statement(ast, target_table.clone(), &self.function_registry)
            .map_err(planner_to_sql_error)?;
        let mut table_catalogs = Vec::with_capacity(source_tables.len() + 1);
        table_catalogs.push(target_table);
        table_catalogs.extend(source_tables);
        let _ = table_catalogs;
        Ok(plan)
    }

    fn bind_delete(
        &self,
        ast: AstStatement,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        delete: &Delete,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let _ = (ast, session, ctx, delete);
        Err(SqlError::UnsupportedStatement {
            reason: "DELETE is not supported yet".to_owned(),
        })
    }

    fn bind_update(
        &self,
        ast: AstStatement,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        update: &Update,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let _ = (ast, session, ctx, update);
        Err(SqlError::UnsupportedStatement {
            reason: "UPDATE is not supported yet".to_owned(),
        })
    }

    fn bind_merge(
        &self,
        ast: AstStatement,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        merge: &Merge,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let _ = (ast, session, ctx, merge);
        Err(SqlError::UnsupportedStatement {
            reason: "MERGE is not supported yet".to_owned(),
        })
    }

    fn bind_create_table(
        &self,
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
        let mut table_options = create_table_options(&create_table.table_options);
        if let Some(distribution) = &create_table.distributed_by {
            bind_paimon_distribution(distribution, &mut table_options)?;
        }
        let table_location = create_table
            .location
            .clone()
            .or_else(|| table_options.remove("location"));
        let table_schema = TableSchema::new(
            create_table
                .columns
                .iter()
                .map(bind_column_def)
                .collect::<Result<Vec<_>, SqlError>>()?,
        );
        let primary_keys = bind_primary_keys(create_table)?;

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
                    "paimon",
                    Arc::new(schema),
                )
                .with_options(table_options.into_iter().collect())
                .with_constraints(constraints)
                .build(),
            ),
        ))
    }

    fn bind_drop(
        &self,
        session: &LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        object_type: &brewdb_sql_parser::ast::ObjectType,
        names: &[ObjectName],
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let Some(name) = names.first() else {
            return Err(SqlError::InvalidRequest {
                reason: "DROP statement must carry at least one object name".to_string(),
            });
        };

        match object_type {
            brewdb_sql_parser::ast::ObjectType::Table => {
                let table = resolve_table(ctx, session, name)?;
                Ok(DataFusionLogicalPlan::Ddl(DdlStatement::DropTable(
                    DropTable {
                        name: TableReference::full(
                            table.path.catalog(),
                            table.path.database(),
                            table.path.table(),
                        ),
                        if_exists: false,
                        schema: empty_df_schema(),
                    },
                )))
            }
            brewdb_sql_parser::ast::ObjectType::Database => {
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

    fn bind_alter(
        &self,
        session: LogicalPlanningSession,
        ctx: &LogicalPlanningContext<'_>,
        alter_table: &AlterTable,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let _ = (session, ctx, alter_table);
        Err(SqlError::UnsupportedStatement {
            reason: "ALTER TABLE is not supported by DataFusion DDL yet".to_string(),
        })
    }

    fn bind_show_databases(
        &self,
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

    fn bind_show_tables(
        &self,
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

    fn bind_set(
        &self,
        set: &Set,
        _session: LogicalPlanningSession,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        match set {
            Set::SingleAssignment {
                scope,
                variable,
                values,
                ..
            } => {
                let value =
                    values
                        .first()
                        .map(expr_to_string)
                        .ok_or_else(|| SqlError::InvalidRequest {
                            reason: "SET statement must carry at least one value".to_string(),
                        })?;
                let _ = scope;
                Ok(DataFusionLogicalPlan::Statement(
                    DataFusionStatement::SetVariable(SetVariable {
                        variable: object_name_to_string(variable),
                        value,
                    }),
                ))
            }
            Set::SetTimeZone { local, value } => {
                let _ = local;
                Ok(DataFusionLogicalPlan::Statement(
                    DataFusionStatement::SetVariable(SetVariable {
                        variable: "timezone".to_string(),
                        value: expr_to_string(value),
                    }),
                ))
            }
            _ => Err(SqlError::UnsupportedStatement {
                reason: format!("unsupported SET statement `{set}`"),
            }),
        }
    }

    fn bind_use(
        &self,
        session: LogicalPlanningSession,
        use_stmt: &Use,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let database_name = match use_stmt {
            Use::Object(name) | Use::Database(name) | Use::Schema(name) => {
                qualify_database_from_use(&session, name)?
            }
            Use::Default => session.database_name,
            _ => {
                return Err(SqlError::UnsupportedStatement {
                    reason: format!("unsupported USE statement `{use_stmt}`"),
                });
            }
        };

        Ok(DataFusionLogicalPlan::Statement(
            DataFusionStatement::SetVariable(SetVariable {
                variable: "database".to_string(),
                value: format!("{}.{}", session.catalog_name, database_name),
            }),
        ))
    }

    fn bind_explain(
        &self,
        statement: &AstStatement,
        format: Option<AnalyzeFormatKind>,
        ctx: &LogicalPlanningContext<'_>,
    ) -> Result<DataFusionLogicalPlan, SqlError> {
        let inner = statement.clone();
        let input = self.plan(inner, ctx)?;
        let _ = format;
        let stringified_plans = vec![input.to_stringified(PlanType::InitialLogicalPlan)];
        Ok(DataFusionLogicalPlan::Explain(Explain {
            verbose: false,
            explain_format: datafusion_expr::logical_plan::ExplainFormat::Indent,
            plan: Arc::new(input),
            stringified_plans,
            schema: Arc::new(
                DFSchema::try_from(DataFusionLogicalPlan::explain_schema()).map_err(|error| {
                    SqlError::InvalidRequest {
                        reason: error.to_string(),
                    }
                })?,
            ),
            logical_optimization_succeeded: false,
        }))
    }
}

fn planning_session(ctx: &LogicalPlanningContext<'_>) -> Result<LogicalPlanningSession, SqlError> {
    let catalog_name = ctx
        .session
        .catalog_name
        .clone()
        .ok_or(SqlError::MissingDefaultCatalog)?;
    let database_name = ctx
        .session
        .database_name
        .clone()
        .ok_or(SqlError::MissingDefaultDatabase)?;
    Ok(LogicalPlanningSession {
        session_id: ctx.session.session_id,
        user_name: ctx.session.user_name.clone(),
        catalog_name,
        database_name,
    })
}

fn default_function_registry() -> MemoryFunctionRegistry {
    let mut registry = MemoryFunctionRegistry::new();
    datafusion_scalar_functions::register_all(&mut registry)
        .expect("default DataFusion scalar functions must register");
    datafusion_aggregate_functions::register_all(&mut registry)
        .expect("default DataFusion aggregate functions must register");
    registry
}

fn planner_to_sql_error(error: PlannerError) -> SqlError {
    match error {
        PlannerError::InvalidPlan { reason } => SqlError::InvalidRequest { reason },
        PlannerError::UnsupportedPlan { reason } => SqlError::UnsupportedStatement { reason },
    }
}

fn empty_df_schema() -> DFSchemaRef {
    Arc::new(DFSchema::empty())
}

fn show_plan(show: Show) -> DataFusionLogicalPlan {
    extension_plan(LogicalPlanNode::Show(show))
}

fn extension_plan(node: LogicalPlanNode) -> DataFusionLogicalPlan {
    DataFusionLogicalPlan::Extension(Extension {
        node: Arc::new(node),
    })
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

fn qualify_database_name(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<(String, String), SqlError> {
    match name_parts(name)?.as_slice() {
        [database] => Ok((session.catalog_name.clone(), database.clone())),
        [catalog, database] => Ok((catalog.clone(), database.clone())),
        _ => Err(SqlError::InvalidRequest {
            reason: format!("invalid database name `{name}`"),
        }),
    }
}

fn qualify_database_from_use(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<String, SqlError> {
    let (_, database_name) = qualify_database_name(session, name)?;
    Ok(database_name)
}

fn qualify_table_name(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<(String, String, String), SqlError> {
    match name_parts(name)?.as_slice() {
        [table] => Ok((
            session.catalog_name.clone(),
            session.database_name.clone(),
            table.clone(),
        )),
        [database, table] => Ok((
            session.catalog_name.clone(),
            database.clone(),
            table.clone(),
        )),
        [catalog, database, table] => Ok((catalog.clone(), database.clone(), table.clone())),
        _ => Err(SqlError::InvalidRequest {
            reason: format!("invalid table name `{name}`"),
        }),
    }
}

fn resolve_table(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<TableCatalogEntry, SqlError> {
    let (catalog_name, database_name, table_name) = qualify_table_name(session, name)?;
    resolve_table_parts(ctx, &catalog_name, &database_name, &table_name)
}

fn resolve_table_object(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    table: &TableObject,
) -> Result<TableCatalogEntry, SqlError> {
    match table {
        TableObject::TableName(name) => resolve_table(ctx, session, name),
        _ => Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported table target `{table}`"),
        }),
    }
}

fn table_factor_object_name(factor: &TableFactor) -> Option<&ObjectName> {
    match factor {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

fn resolve_query_tables(
    ctx: &LogicalPlanningContext<'_>,
    session: &LogicalPlanningSession,
    query: &Query,
) -> Result<Vec<TableCatalogEntry>, SqlError> {
    let mut names = Vec::new();
    collect_query_table_names(query, &mut names)?;
    let mut seen = BTreeSet::new();
    let mut tables = Vec::new();
    for name in names {
        let table = resolve_table(ctx, session, &name)?;
        if seen.insert(table.table_id) {
            tables.push(table);
        }
    }
    Ok(tables)
}

fn collect_query_table_names(query: &Query, names: &mut Vec<ObjectName>) -> Result<(), SqlError> {
    if query.with.is_some() {
        return Err(SqlError::UnsupportedStatement {
            reason: "WITH queries are not supported yet".to_string(),
        });
    }
    collect_set_expr_table_names(query.body.as_ref(), names)
}

fn collect_set_expr_table_names(
    set_expr: &SetExpr,
    names: &mut Vec<ObjectName>,
) -> Result<(), SqlError> {
    match set_expr {
        SetExpr::Select(select) => {
            for from in &select.from {
                collect_table_factor_names(&from.relation, names)?;
                for join in &from.joins {
                    collect_table_factor_names(&join.relation, names)?;
                }
            }
            Ok(())
        }
        SetExpr::Query(query) => collect_query_table_names(query, names),
        SetExpr::Values(_) => Ok(()),
        other => Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

fn collect_table_factor_names(
    factor: &TableFactor,
    names: &mut Vec<ObjectName>,
) -> Result<(), SqlError> {
    let Some(name) = table_factor_object_name(factor) else {
        return Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported table factor `{factor}`"),
        });
    };
    names.push(name.clone());
    Ok(())
}

fn resolve_table_parts(
    ctx: &LogicalPlanningContext<'_>,
    catalog_name: &str,
    database_name: &str,
    table_name: &str,
) -> Result<TableCatalogEntry, SqlError> {
    let path = CatalogPath::new(catalog_name).map_err(|error| SqlError::InvalidRequest {
        reason: error.to_string(),
    })?;
    let catalog = ctx
        .catalog_service
        .open_catalog(path.catalog())
        .map_err(|error| SqlError::InvalidRequest {
            reason: error.to_string(),
        })?;
    catalog
        .get_table(database_name, table_name)
        .map_err(|error| SqlError::InvalidRequest {
            reason: error.to_string(),
        })
}

fn name_parts(name: &ObjectName) -> Result<Vec<String>, SqlError> {
    name.0
        .iter()
        .map(object_name_part_value)
        .collect::<Result<Vec<_>, _>>()
}

fn object_name_part_value(part: &ObjectNamePart) -> Result<String, SqlError> {
    match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(SqlError::UnsupportedStatement {
            reason: "dynamic object names are not supported".to_string(),
        }),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".")
}

fn ident_list(idents: &[Ident]) -> String {
    idents
        .iter()
        .map(|ident| ident.value.clone())
        .collect::<Vec<_>>()
        .join(".")
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

fn primary_key_column_name(
    column: &brewdb_sql_parser::ast::IndexColumn,
) -> Result<String, SqlError> {
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
                brewdb_sql_parser::ast::TimezoneInfo::None
                    | brewdb_sql_parser::ast::TimezoneInfo::WithoutTimeZone
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

fn timezone_precision(
    timezone: &brewdb_sql_parser::ast::TimezoneInfo,
    default_precision: u32,
) -> u32 {
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

fn bind_paimon_distribution(
    distribution: &brewdb_sql_parser::ast::DistributedBy,
    table_options: &mut BTreeMap<String, String>,
) -> Result<(), SqlError> {
    if !distribution.columns.is_empty() {
        table_options.insert(
            "bucket-key".to_string(),
            distribution
                .columns
                .iter()
                .map(|column| column.value.clone())
                .collect::<Vec<_>>()
                .join(","),
        );
    } else if distribution.function.is_some() {
        return Err(SqlError::InvalidRequest {
            reason: "Paimon bucket function requires at least one bucket key".to_string(),
        });
    }

    if let Some(function) = &distribution.function {
        match function.value.to_ascii_lowercase().as_str() {
            "default" | "hash" => {
                table_options.remove("bucket-function.type");
            }
            "mod" => {
                table_options.insert("bucket-function.type".to_string(), "mod".to_string());
            }
            "hive" => {
                table_options.insert("bucket-function.type".to_string(), "hive".to_string());
            }
            other => {
                return Err(SqlError::InvalidRequest {
                    reason: format!(
                        "unsupported Paimon bucket function `{other}`; supported functions are default, hash, mod, hive"
                    ),
                });
            }
        }
    }

    if let Some(bucket_count) = distribution.buckets {
        table_options.insert("bucket".to_string(), bucket_count.to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use brewdb_catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, LakeFormatKind,
        open_catalog_store,
    };
    use brewdb_common::config::{ConfigPatch, ConfigScope, global_config_registry};
    use brewdb_common::defaults::MANAGED_PAIMON_CATALOG_NAME;
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_sql::{SqlParser, SqlRequestContext, SqlSessionContext};
    use uuid::Uuid;

    use crate::logical::plan::{CreateDatabase, Ddl, DropDatabase, LogicalPlanNode, Show};
    use crate::logical::{LogicalOptimizer, LogicalPlanningContext};

    use super::LogicalPlanner;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).expect("test directory must be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn catalog_service() -> CatalogService {
        let store = open_catalog_store(&CatalogConfig {
            store_backend: CatalogStoreBackendKind::Memory,
            paimon_warehouse: String::new(),
        });
        let warehouse = TestDir::new("brewdb-planner-logical_planner-tests");
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.path().to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();
        let service = CatalogService::with_config(store, config);
        let entry = CatalogEntry::new(
            Uuid::new_v4(),
            CatalogPath::new(MANAGED_PAIMON_CATALOG_NAME).unwrap(),
            CatalogMode::Managed,
            LakeFormatKind::Paimon,
        );
        service.create_catalog(entry).unwrap();
        let catalog = service.open_catalog(MANAGED_PAIMON_CATALOG_NAME).unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("brewdb"))
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "orders",
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        catalog
            .create_table(
                CreateTableRequest::new(
                    "brewdb",
                    "customers",
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        service
    }

    fn bind(sql: &str) -> datafusion_expr::LogicalPlan {
        let service = catalog_service();
        let parsed = SqlParser.sql_to_statement(sql).unwrap();
        LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    session: &SqlSessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                    },
                    request: &SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap()
    }

    #[test]
    fn logical_planner_turns_select_into_datafusion_logical_plan() {
        let planned = bind("select * from orders");

        match planned {
            datafusion_expr::LogicalPlan::Projection(_)
            | datafusion_expr::LogicalPlan::TableScan(_) => {}
            other => panic!("expected DataFusion logical plan, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_insert_into_datafusion_logical_plan() {
        let planned = bind("insert into orders values (1), (2)");

        match planned {
            datafusion_expr::LogicalPlan::Dml(_) => {}
            other => panic!("expected DataFusion logical plan, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_create_table_distribution_into_paimon_options() {
        let planned = bind(
            "create table t1 (id int not null, name text) distributed by mod(id) into 8 buckets",
        );

        match planned {
            datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::CreateExternalTable(statement),
            ) => {
                assert_eq!(
                    statement.name.to_string(),
                    "managed_paimon_catalog.brewdb.t1"
                );
                assert_eq!(statement.file_type, "paimon");
                assert_eq!(statement.schema.fields().len(), 2);
                assert!(!statement.schema.field(0).is_nullable());
                assert_eq!(
                    statement
                        .options
                        .get("bucket-function.type")
                        .map(String::as_str),
                    Some("mod")
                );
                assert_eq!(
                    statement.options.get("bucket").map(String::as_str),
                    Some("8")
                );
            }
            other => panic!("expected create table statement, got {other:?}"),
        }
    }

    #[test]
    fn logical_planner_turns_database_ddl_into_brewdb_extension_plans() {
        let planned = bind("create database wl_db");
        assert!(matches!(
            brewdb_logical_node(&planned),
            Some(LogicalPlanNode::Ddl(Ddl::CreateDatabase(CreateDatabase {
                catalog_name,
                database_name,
            }))) if catalog_name == MANAGED_PAIMON_CATALOG_NAME && database_name == "wl_db"
        ));

        let planned = bind("drop database brewdb");
        assert!(matches!(
            brewdb_logical_node(&planned),
            Some(LogicalPlanNode::Ddl(Ddl::DropDatabase(DropDatabase {
                catalog_name,
                database_name,
            }))) if catalog_name == MANAGED_PAIMON_CATALOG_NAME && database_name == "brewdb"
        ));
    }

    #[test]
    fn logical_planner_keeps_table_ddl_as_datafusion_ddl() {
        let planned = bind("drop table orders");
        assert!(matches!(
            planned,
            datafusion_expr::LogicalPlan::Ddl(datafusion_expr::DdlStatement::DropTable(_))
        ));
    }

    #[test]
    fn logical_planner_rejects_invalid_create_table_shapes() {
        let service = catalog_service();
        let parsed = SqlParser.sql_to_statement("create table t1").unwrap();
        let error = LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    session: &SqlSessionContext {
                        session_id: Uuid::nil(),
                        user_name: "brew".to_owned(),
                        catalog_name: Some(MANAGED_PAIMON_CATALOG_NAME.to_owned()),
                        database_name: Some("brewdb".to_owned()),
                    },
                    request: &SqlRequestContext {
                        request_id: Uuid::nil(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid sql request: CREATE TABLE must define at least one column"
        );
    }

    #[test]
    fn logical_planner_turns_show_and_set_into_logical_plans() {
        let planned = bind("show catalogs");
        assert!(matches!(brewdb_show_plan(&planned), Some(Show::Catalogs)));

        let planned = bind("show tables");
        match planned {
            datafusion_expr::LogicalPlan::Extension(_) => {
                let Some(Show::Tables {
                    catalog_name,
                    database_name,
                }) = brewdb_show_plan(&planned)
                else {
                    panic!("expected SHOW TABLES extension plan");
                };
                assert_eq!(catalog_name, MANAGED_PAIMON_CATALOG_NAME);
                assert_eq!(database_name, "brewdb");
            }
            other => panic!("expected show tables statement, got {other:?}"),
        }

        let planned = bind("set work_mem = '128MB'");
        assert!(matches!(
            planned,
            datafusion_expr::LogicalPlan::Statement(datafusion_expr::Statement::SetVariable(_))
        ));
    }

    #[test]
    fn logical_optimizer_applies_brewdb_extension_rules() {
        let planned = bind("show catalogs");
        let mut observed_rules = Vec::new();

        let optimized = LogicalOptimizer::default()
            .optimize_with_observer(planned, |_, rule| {
                observed_rules.push(rule.name().to_string());
            })
            .unwrap();

        assert!(matches!(brewdb_show_plan(&optimized), Some(Show::Catalogs)));
        assert!(
            observed_rules
                .iter()
                .any(|rule| rule == "brewdb_logical_extension")
        );
    }

    fn brewdb_show_plan(plan: &datafusion_expr::LogicalPlan) -> Option<&Show> {
        brewdb_logical_node(plan).and_then(LogicalPlanNode::show)
    }

    fn brewdb_logical_node(plan: &datafusion_expr::LogicalPlan) -> Option<&LogicalPlanNode> {
        let datafusion_expr::LogicalPlan::Extension(extension) = plan else {
            return None;
        };
        extension.node.as_any().downcast_ref::<LogicalPlanNode>()
    }
}

fn bind_txn_access_mode(modes: &[AstTransactionMode]) -> DataFusionTransactionAccessMode {
    if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
        )
    }) {
        DataFusionTransactionAccessMode::ReadOnly
    } else if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
        )
    }) {
        DataFusionTransactionAccessMode::ReadWrite
    } else {
        DataFusionTransactionAccessMode::ReadWrite
    }
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => value_to_string(value),
        _ => expr.to_string(),
    }
}

fn value_to_string(value: &ValueWithSpan) -> String {
    match &value.value {
        Value::SingleQuotedString(inner)
        | Value::DoubleQuotedString(inner)
        | Value::EscapedStringLiteral(inner)
        | Value::NationalStringLiteral(inner)
        | Value::HexStringLiteral(inner)
        | Value::SingleQuotedByteStringLiteral(inner)
        | Value::DoubleQuotedByteStringLiteral(inner)
        | Value::SingleQuotedRawStringLiteral(inner)
        | Value::DoubleQuotedRawStringLiteral(inner)
        | Value::TripleSingleQuotedString(inner)
        | Value::TripleDoubleQuotedString(inner)
        | Value::TripleSingleQuotedRawStringLiteral(inner)
        | Value::TripleDoubleQuotedRawStringLiteral(inner)
        | Value::UnicodeStringLiteral(inner)
        | Value::TripleSingleQuotedByteStringLiteral(inner)
        | Value::TripleDoubleQuotedByteStringLiteral(inner) => inner.clone(),
        _ => value.to_string(),
    }
}
