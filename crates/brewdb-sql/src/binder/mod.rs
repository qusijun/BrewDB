//! SQL binder from parsed statements into BrewDB bound statement contracts.

pub mod context;

use std::collections::{BTreeMap, BTreeSet};

use brewdb_catalog::{CatalogPath, TableCatalogEntry};
use brewdb_common::schema::{DataType, SchemaField, TableSchema};
use datafusion_sql::sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation as AstAlterTableOperation, AnalyzeFormat,
    AnalyzeFormatKind, ColumnDef, ContextModifier, CreateTable, CreateTableOptions,
    DataType as AstDataType, Delete, ExactNumberInfo, Expr, FromTable, Ident, Insert, Merge,
    ObjectName, ObjectNamePart, Query, Set, SetExpr, SqlOption, Statement as AstStatement,
    TableFactor, TableObject, TableWithJoins, TransactionAccessMode,
    TransactionMode as AstTransactionMode, Update, Use, Value, ValueWithSpan,
};

use crate::errors::SqlError;
use crate::statement::{
    BoundAlterStatement, BoundAlterTableOperation, BoundAlterTableStatement, BoundBeginStatement,
    BoundCommitStatement, BoundCreateDatabaseStatement, BoundCreateStatement,
    BoundCreateTableStatement, BoundDeleteStatement, BoundDropDatabaseStatement,
    BoundDropStatement, BoundDropTableStatement, BoundExplainStatement, BoundInsertStatement,
    BoundMergeStatement, BoundPlanStatement, BoundQueryStatement, BoundRollbackStatement,
    BoundSessionContext, BoundSetStatement, BoundSetVariableStatement, BoundStatement,
    BoundTransactionStatement, BoundUpdateStatement, BoundUseDatabaseStatement, ExplainKind,
    ParsedStatement, ParsedStatementKind, SetScope, TransactionMode,
};

use self::context::StatementBindingContext;

#[derive(Clone, Debug, Default)]
pub struct SqlBinder;

impl SqlBinder {
    pub fn bind(
        &self,
        parsed: ParsedStatement,
        ctx: &StatementBindingContext<'_>,
    ) -> Result<BoundStatement, SqlError> {
        let bound_session = bound_session(ctx)?;
        let ast = parsed.ast.clone();
        match &ast {
            AstStatement::Query(query) => self.bind_query(parsed, bound_session, ctx, query),
            AstStatement::Insert(insert) => self.bind_insert(parsed, bound_session, ctx, insert),
            AstStatement::Delete(delete) => self.bind_delete(parsed, bound_session, ctx, delete),
            AstStatement::Update(update) => self.bind_update(parsed, bound_session, ctx, update),
            AstStatement::Merge(merge) => self.bind_merge(parsed, bound_session, ctx, merge),
            AstStatement::CreateDatabase { db_name, .. } => Ok(BoundStatement::Create(
                BoundCreateStatement::Database(BoundCreateDatabaseStatement {
                    catalog_name: bound_session.catalog_name.clone(),
                    database_name: db_name.to_string(),
                    options: BTreeMap::new(),
                }),
            )),
            AstStatement::CreateTable(create_table) => {
                self.bind_create_table(&bound_session, create_table)
            }
            AstStatement::Drop {
                object_type, names, ..
            } => self.bind_drop(&bound_session, ctx, object_type, names),
            AstStatement::AlterTable(alter_table) => {
                self.bind_alter(bound_session, ctx, alter_table)
            }
            AstStatement::Set(set) => self.bind_set(set, bound_session),
            AstStatement::Use(use_stmt) => self.bind_use(bound_session, use_stmt),
            AstStatement::StartTransaction { modes, .. } => Ok(BoundStatement::Transaction(
                BoundTransactionStatement::Begin(BoundBeginStatement {
                    mode: bind_txn_mode(modes),
                }),
            )),
            AstStatement::Commit { .. } => Ok(BoundStatement::Transaction(
                BoundTransactionStatement::Commit(BoundCommitStatement),
            )),
            AstStatement::Rollback { .. } => Ok(BoundStatement::Transaction(
                BoundTransactionStatement::Rollback(BoundRollbackStatement),
            )),
            AstStatement::Explain {
                statement, format, ..
            } => self.bind_explain(statement, *format, ctx),
            _ => Err(SqlError::UnsupportedStatement {
                reason: parsed.statement_text,
            }),
        }
    }

    fn bind_query(
        &self,
        parsed: ParsedStatement,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        query: &Query,
    ) -> Result<BoundStatement, SqlError> {
        let tables = resolve_query_tables(ctx, &session, query)?;
        Ok(BoundStatement::Plan(BoundPlanStatement::Query(
            BoundQueryStatement {
                statement_text: parsed.statement_text,
                session,
                tables,
                ast: parsed.ast,
            },
        )))
    }

    fn bind_insert(
        &self,
        parsed: ParsedStatement,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        insert: &Insert,
    ) -> Result<BoundStatement, SqlError> {
        let target_table = resolve_table_object(ctx, &session, &insert.table)?;
        let source_tables = insert
            .source
            .as_deref()
            .map(|query| resolve_query_tables(ctx, &session, query))
            .transpose()?
            .unwrap_or_default();
        Ok(BoundStatement::Plan(BoundPlanStatement::Insert(
            BoundInsertStatement {
                statement_text: parsed.statement_text,
                session,
                target_table,
                source_tables,
                ast: parsed.ast,
            },
        )))
    }

    fn bind_delete(
        &self,
        parsed: ParsedStatement,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        delete: &Delete,
    ) -> Result<BoundStatement, SqlError> {
        let target_table = resolve_delete_target(ctx, &session, &delete.from)?;
        Ok(BoundStatement::Plan(BoundPlanStatement::Delete(
            BoundDeleteStatement {
                statement_text: parsed.statement_text,
                session,
                target_table,
                ast: parsed.ast,
            },
        )))
    }

    fn bind_update(
        &self,
        parsed: ParsedStatement,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        update: &Update,
    ) -> Result<BoundStatement, SqlError> {
        let target_table = resolve_table_factor(ctx, &session, &update.table.relation)?;
        let source_tables = update
            .from
            .as_ref()
            .map(resolve_update_sources)
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .map(|name| resolve_table(ctx, &session, &name))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(BoundStatement::Plan(BoundPlanStatement::Update(
            BoundUpdateStatement {
                statement_text: parsed.statement_text,
                session,
                target_table,
                source_tables,
                ast: parsed.ast,
            },
        )))
    }

    fn bind_merge(
        &self,
        parsed: ParsedStatement,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        merge: &Merge,
    ) -> Result<BoundStatement, SqlError> {
        let target_table = resolve_table_factor(ctx, &session, &merge.table)?;
        let source_tables = table_factor_object_name(&merge.source)
            .map(|name| resolve_table(ctx, &session, name))
            .transpose()?
            .into_iter()
            .collect();

        Ok(BoundStatement::Plan(BoundPlanStatement::Merge(
            BoundMergeStatement {
                statement_text: parsed.statement_text,
                session,
                target_table,
                source_tables,
                ast: parsed.ast,
            },
        )))
    }

    fn bind_create_table(
        &self,
        session: &BoundSessionContext,
        create_table: &CreateTable,
    ) -> Result<BoundStatement, SqlError> {
        let (catalog_name, database_name, table_name) =
            qualify_table_name(session, &create_table.name)?;
        let mut table_options = create_table_options(&create_table.table_options);
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

        Ok(BoundStatement::Create(BoundCreateStatement::Table(
            BoundCreateTableStatement {
                catalog_name,
                database_name,
                table_name,
                table_schema,
                table_location,
                table_options,
            },
        )))
    }

    fn bind_drop(
        &self,
        session: &BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        object_type: &datafusion_sql::sqlparser::ast::ObjectType,
        names: &[ObjectName],
    ) -> Result<BoundStatement, SqlError> {
        let Some(name) = names.first() else {
            return Err(SqlError::InvalidRequest {
                reason: "DROP statement must carry at least one object name".to_string(),
            });
        };

        match object_type {
            datafusion_sql::sqlparser::ast::ObjectType::Table => Ok(BoundStatement::Drop(
                BoundDropStatement::Table(BoundDropTableStatement {
                    table: resolve_table(ctx, session, name)?,
                }),
            )),
            datafusion_sql::sqlparser::ast::ObjectType::Database
            | datafusion_sql::sqlparser::ast::ObjectType::Schema => {
                let (catalog_name, database_name) = qualify_database_name(session, name)?;
                Ok(BoundStatement::Drop(BoundDropStatement::Database(
                    BoundDropDatabaseStatement {
                        catalog_name,
                        database_name,
                    },
                )))
            }
            _ => Err(SqlError::UnsupportedStatement {
                reason: format!("DROP {object_type}"),
            }),
        }
    }

    fn bind_alter(
        &self,
        session: BoundSessionContext,
        ctx: &StatementBindingContext<'_>,
        alter_table: &AlterTable,
    ) -> Result<BoundStatement, SqlError> {
        let table = resolve_table(ctx, &session, &alter_table.name)?;
        let operations = alter_table
            .operations
            .iter()
            .map(bind_alter_operation)
            .collect::<Result<Vec<_>, SqlError>>()?;
        Ok(BoundStatement::Alter(BoundAlterStatement::Table(
            BoundAlterTableStatement { table, operations },
        )))
    }

    fn bind_set(
        &self,
        set: &Set,
        _session: BoundSessionContext,
    ) -> Result<BoundStatement, SqlError> {
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
                Ok(BoundStatement::Set(BoundSetStatement::Variable(
                    BoundSetVariableStatement {
                        scope: bind_set_scope(*scope),
                        key: object_name_to_string(variable),
                        value,
                    },
                )))
            }
            Set::SetTimeZone { local, value } => Ok(BoundStatement::Set(
                BoundSetStatement::Variable(BoundSetVariableStatement {
                    scope: if *local {
                        SetScope::Session
                    } else {
                        SetScope::System
                    },
                    key: "timezone".to_string(),
                    value: expr_to_string(value),
                }),
            )),
            _ => Err(SqlError::UnsupportedStatement {
                reason: format!("unsupported SET statement `{set}`"),
            }),
        }
    }

    fn bind_use(
        &self,
        session: BoundSessionContext,
        use_stmt: &Use,
    ) -> Result<BoundStatement, SqlError> {
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

        Ok(BoundStatement::Set(BoundSetStatement::Database(
            BoundUseDatabaseStatement {
                catalog_name: session.catalog_name,
                database_name,
            },
        )))
    }

    fn bind_explain(
        &self,
        statement: &AstStatement,
        format: Option<AnalyzeFormatKind>,
        ctx: &StatementBindingContext<'_>,
    ) -> Result<BoundStatement, SqlError> {
        let inner = ParsedStatement {
            statement_text: statement.to_string(),
            kind: ParsedStatementKind::from_ast(statement),
            ast: statement.clone(),
        };
        let statement = self.bind(inner, ctx)?;
        Ok(BoundStatement::Explain(BoundExplainStatement {
            kind: match format.map(analyze_format) {
                Some(AnalyzeFormat::GRAPHVIZ) | Some(AnalyzeFormat::JSON) => ExplainKind::Physical,
                _ => ExplainKind::Distributed,
            },
            statement: Box::new(statement),
        }))
    }
}

fn bound_session(ctx: &StatementBindingContext<'_>) -> Result<BoundSessionContext, SqlError> {
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
    Ok(BoundSessionContext {
        session_id: ctx.session.session_id,
        user_name: ctx.session.user_name.clone(),
        catalog_name,
        database_name,
    })
}

fn qualify_database_name(
    session: &BoundSessionContext,
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
    session: &BoundSessionContext,
    name: &ObjectName,
) -> Result<String, SqlError> {
    let (_, database_name) = qualify_database_name(session, name)?;
    Ok(database_name)
}

fn qualify_table_name(
    session: &BoundSessionContext,
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
    ctx: &StatementBindingContext<'_>,
    session: &BoundSessionContext,
    name: &ObjectName,
) -> Result<TableCatalogEntry, SqlError> {
    let (catalog_name, database_name, table_name) = qualify_table_name(session, name)?;
    resolve_table_parts(ctx, &catalog_name, &database_name, &table_name)
}

fn resolve_table_object(
    ctx: &StatementBindingContext<'_>,
    session: &BoundSessionContext,
    table: &TableObject,
) -> Result<TableCatalogEntry, SqlError> {
    match table {
        TableObject::TableName(name) => resolve_table(ctx, session, name),
        _ => Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported table target `{table}`"),
        }),
    }
}

fn resolve_delete_target(
    ctx: &StatementBindingContext<'_>,
    session: &BoundSessionContext,
    from: &FromTable,
) -> Result<TableCatalogEntry, SqlError> {
    let tables = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    let Some(table) = tables.first() else {
        return Err(SqlError::InvalidRequest {
            reason: "DELETE statement must carry a target table".to_string(),
        });
    };
    resolve_table_factor(ctx, session, &table.relation)
}

fn resolve_table_factor(
    ctx: &StatementBindingContext<'_>,
    session: &BoundSessionContext,
    factor: &TableFactor,
) -> Result<TableCatalogEntry, SqlError> {
    let Some(name) = table_factor_object_name(factor) else {
        return Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported table factor `{factor}`"),
        });
    };
    resolve_table(ctx, session, name)
}

fn table_factor_object_name(factor: &TableFactor) -> Option<&ObjectName> {
    match factor {
        TableFactor::Table { name, .. } => Some(name),
        _ => None,
    }
}

fn resolve_update_sources(
    from: &datafusion_sql::sqlparser::ast::UpdateTableFromKind,
) -> Result<Vec<ObjectName>, SqlError> {
    let tables = match from {
        datafusion_sql::sqlparser::ast::UpdateTableFromKind::BeforeSet(tables)
        | datafusion_sql::sqlparser::ast::UpdateTableFromKind::AfterSet(tables) => tables,
    };

    tables
        .iter()
        .map(table_with_joins_name)
        .collect::<Result<Vec<_>, _>>()
}

fn resolve_query_tables(
    ctx: &StatementBindingContext<'_>,
    session: &BoundSessionContext,
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

fn table_with_joins_name(table: &TableWithJoins) -> Result<ObjectName, SqlError> {
    let Some(name) = table_factor_object_name(&table.relation) else {
        return Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported joined table source `{table}`"),
        });
    };
    Ok(name.clone())
}

fn resolve_table_parts(
    ctx: &StatementBindingContext<'_>,
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

fn bind_column_def(column: &ColumnDef) -> Result<SchemaField, SqlError> {
    let mut bound = SchemaField::new(
        column.name.value.clone(),
        bind_data_type(&column.data_type)?,
    );
    bound.nullable = !column.options.iter().any(|option| {
        matches!(
            option.option,
            datafusion_sql::sqlparser::ast::ColumnOption::NotNull
        )
    });
    Ok(bound)
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
                datafusion_sql::sqlparser::ast::TimezoneInfo::None
                    | datafusion_sql::sqlparser::ast::TimezoneInfo::WithoutTimeZone
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
    timezone: &datafusion_sql::sqlparser::ast::TimezoneInfo,
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

fn bind_alter_operation(
    operation: &AstAlterTableOperation,
) -> Result<BoundAlterTableOperation, SqlError> {
    match operation {
        AstAlterTableOperation::AddColumn { column_def, .. } => Ok(
            BoundAlterTableOperation::AddColumn(bind_column_def(column_def)?),
        ),
        AstAlterTableOperation::DropColumn { column_names, .. } => {
            let Some(column_name) = column_names.first() else {
                return Err(SqlError::InvalidRequest {
                    reason: "ALTER TABLE DROP COLUMN requires at least one column name".to_string(),
                });
            };
            Ok(BoundAlterTableOperation::DropColumn {
                column_name: ident_value(column_name),
            })
        }
        AstAlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => Ok(BoundAlterTableOperation::RenameColumn {
            old_name: ident_value(old_column_name),
            new_name: ident_value(new_column_name),
        }),
        AstAlterTableOperation::AlterColumn { column_name, op } => match op {
            AlterColumnOperation::SetDataType { data_type, .. } => {
                Ok(BoundAlterTableOperation::AlterColumnType {
                    column_name: ident_value(column_name),
                    data_type: bind_data_type(data_type)?,
                })
            }
            _ => Err(SqlError::UnsupportedStatement {
                reason: format!("unsupported alter column operation `{op}`"),
            }),
        },
        AstAlterTableOperation::SetTblProperties { table_properties } => {
            let Some((key, value)) = table_properties.first().and_then(sql_option_entry) else {
                return Err(SqlError::InvalidRequest {
                    reason: "ALTER TABLE SET TBLPROPERTIES requires at least one property"
                        .to_string(),
                });
            };
            Ok(BoundAlterTableOperation::SetTableOption { key, value })
        }
        _ => Err(SqlError::UnsupportedStatement {
            reason: format!("unsupported alter table operation `{operation}`"),
        }),
    }
}

fn bind_txn_mode(modes: &[AstTransactionMode]) -> TransactionMode {
    if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
        )
    }) {
        TransactionMode::ReadOnly
    } else if modes.iter().any(|mode| {
        matches!(
            mode,
            AstTransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
        )
    }) {
        TransactionMode::ReadWrite
    } else {
        TransactionMode::Unspecified
    }
}

fn bind_set_scope(scope: Option<ContextModifier>) -> SetScope {
    match scope {
        Some(ContextModifier::Local | ContextModifier::Session) => SetScope::Session,
        Some(ContextModifier::Global) | None => SetScope::System,
    }
}

fn analyze_format(kind: AnalyzeFormatKind) -> AnalyzeFormat {
    match kind {
        AnalyzeFormatKind::Keyword(format) | AnalyzeFormatKind::Assignment(format) => format,
    }
}

fn ident_value(ident: &Ident) -> String {
    ident.value.clone()
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
