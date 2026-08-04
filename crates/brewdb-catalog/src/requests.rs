//! Catalog-facing request models.

use std::collections::BTreeMap;

use brewdb_common::schema::{ColumnSchema, DataType, TableSchema};

pub type ColumnDefinition = ColumnSchema;
pub type TableDefinition = TableSchema;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseRequest {
    pub database_name: String,
    pub database_options: BTreeMap<String, String>,
}

impl CreateDatabaseRequest {
    pub fn new(database_name: impl Into<String>) -> Self {
        Self {
            database_name: database_name.into(),
            database_options: BTreeMap::new(),
        }
    }

    pub fn with_options(
        mut self,
        database_options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.database_options = database_options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableRequest {
    pub database_name: String,
    pub table_name: String,
    pub table_schema: TableDefinition,
    pub table_location: Option<String>,
    pub table_options: BTreeMap<String, String>,
}

impl CreateTableRequest {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        table_schema: TableDefinition,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            table_name: table_name.into(),
            table_schema,
            table_location: None,
            table_options: BTreeMap::new(),
        }
    }

    pub fn with_location(mut self, table_location: impl Into<String>) -> Self {
        self.table_location = Some(table_location.into());
        self
    }

    pub fn with_options(
        mut self,
        table_options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.table_options = table_options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameTableRequest {
    pub database_name: String,
    pub table_name: String,
    pub new_database_name: String,
    pub new_table_name: String,
}

impl RenameTableRequest {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        new_database_name: impl Into<String>,
        new_table_name: impl Into<String>,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            table_name: table_name.into(),
            new_database_name: new_database_name.into(),
            new_table_name: new_table_name.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterTableOperation {
    AddColumn(ColumnDefinition),
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
pub struct AlterTableRequest {
    pub database_name: String,
    pub table_name: String,
    pub operations: Vec<AlterTableOperation>,
}

impl AlterTableRequest {
    pub fn new(
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        operations: Vec<AlterTableOperation>,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            table_name: table_name.into(),
            operations,
        }
    }
}
