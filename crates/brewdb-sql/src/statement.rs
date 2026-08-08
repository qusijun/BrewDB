//! SQL statement contracts from parser output through binding output.

use std::collections::BTreeMap;

use brewdb_catalog::TableCatalogEntry;
use brewdb_common::schema::{DataType, SchemaField, TableSchema};
use datafusion_sql::sqlparser::ast::{Set as AstSet, Statement as AstStatement};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParsedStatementKind {
    Query,
    Insert,
    Delete,
    Update,
    Merge,
    Create,
    Drop,
    Alter,
    Set,
    Transaction,
    Explain,
    Unsupported,
}

impl ParsedStatementKind {
    pub fn from_ast(ast: &AstStatement) -> Self {
        match ast {
            AstStatement::Query(_) => Self::Query,
            AstStatement::Insert(_) => Self::Insert,
            AstStatement::Delete(_) => Self::Delete,
            AstStatement::Update(_) => Self::Update,
            AstStatement::Merge(_) => Self::Merge,
            AstStatement::CreateTable(_) | AstStatement::CreateDatabase { .. } => Self::Create,
            AstStatement::Drop { .. } => Self::Drop,
            AstStatement::AlterTable(_) => Self::Alter,
            AstStatement::Set(AstSet::SingleAssignment { .. })
            | AstStatement::Set(AstSet::SetTimeZone { .. })
            | AstStatement::Use(_) => Self::Set,
            AstStatement::StartTransaction { .. }
            | AstStatement::Commit { .. }
            | AstStatement::Rollback { .. } => Self::Transaction,
            AstStatement::Explain { .. } => Self::Explain,
            _ => Self::Unsupported,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedStatement {
    pub statement_text: String,
    pub kind: ParsedStatementKind,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSessionContext {
    pub session_id: Uuid,
    pub user_name: String,
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundStatement {
    Plan(BoundPlanStatement),
    Create(BoundCreateStatement),
    Drop(BoundDropStatement),
    Alter(BoundAlterStatement),
    Set(BoundSetStatement),
    Transaction(BoundTransactionStatement),
    Explain(BoundExplainStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundPlanStatement {
    Query(BoundQueryStatement),
    Insert(BoundInsertStatement),
    Delete(BoundDeleteStatement),
    Update(BoundUpdateStatement),
    Merge(BoundMergeStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundQueryStatement {
    pub statement_text: String,
    pub session: BoundSessionContext,
    pub tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundInsertStatement {
    pub statement_text: String,
    pub session: BoundSessionContext,
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDeleteStatement {
    pub statement_text: String,
    pub session: BoundSessionContext,
    pub target_table: TableCatalogEntry,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundUpdateStatement {
    pub statement_text: String,
    pub session: BoundSessionContext,
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundMergeStatement {
    pub statement_text: String,
    pub session: BoundSessionContext,
    pub target_table: TableCatalogEntry,
    pub source_tables: Vec<TableCatalogEntry>,
    pub ast: AstStatement,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundCreateStatement {
    Database(BoundCreateDatabaseStatement),
    Table(BoundCreateTableStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCreateDatabaseStatement {
    pub catalog_name: String,
    pub database_name: String,
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCreateTableStatement {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
    pub table_schema: TableSchema,
    pub table_location: Option<String>,
    pub table_options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundDropStatement {
    Database(BoundDropDatabaseStatement),
    Table(BoundDropTableStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDropDatabaseStatement {
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundDropTableStatement {
    pub table: TableCatalogEntry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundAlterStatement {
    Table(BoundAlterTableStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundAlterTableStatement {
    pub table: TableCatalogEntry,
    pub operations: Vec<BoundAlterTableOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundAlterTableOperation {
    AddColumn(SchemaField),
    DropColumn {
        column_name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    AlterColumnType {
        column_name: String,
        data_type: DataType,
    },
    SetTableOption {
        key: String,
        value: String,
    },
    RemoveTableOption {
        key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundSetStatement {
    Variable(BoundSetVariableStatement),
    Database(BoundUseDatabaseStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSetVariableStatement {
    pub scope: SetScope,
    pub key: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetScope {
    Session,
    System,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundUseDatabaseStatement {
    pub catalog_name: String,
    pub database_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BoundTransactionStatement {
    Begin(BoundBeginStatement),
    Commit(BoundCommitStatement),
    Rollback(BoundRollbackStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundBeginStatement {
    pub mode: TransactionMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundCommitStatement;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundRollbackStatement;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionMode {
    ReadOnly,
    ReadWrite,
    Unspecified,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundExplainStatement {
    pub kind: ExplainKind,
    pub statement: Box<BoundStatement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainKind {
    Logical,
    Physical,
    Distributed,
}
