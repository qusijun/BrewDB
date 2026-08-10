//! SQL-to-runtime driver.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use brewdb_catalog::{
    AlterTableOperation, AlterTableRequest, CatalogError, CatalogService, CreateDatabaseRequest,
    CreateTableRequest,
};
use brewdb_common::runtime::QueryContext;
use brewdb_planner::{DistributedPlanner, DistributedPlannerRequest, PlannerError};
use brewdb_sql::{
    BoundAlterStatement, BoundAlterTableOperation, BoundAlterTableStatement,
    BoundCreateDatabaseStatement, BoundCreateStatement, BoundCreateTableStatement,
    BoundDropDatabaseStatement, BoundDropStatement, BoundDropTableStatement, BoundShowStatement,
    BoundStatement, SqlBinder, SqlError, SqlIngressRequest, SqlParser,
    binder::context::StatementBindingContext,
};

use crate::execution::{
    DataFusionExecutionRuntime, ExecutionRuntime, ExecutionRuntimeError, QueryExecutionHandle,
    QueryExecutionRequest,
};

#[derive(Debug)]
pub enum SqlDriverError {
    Catalog(CatalogError),
    Sql(SqlError),
    Planner(PlannerError),
    Runtime(ExecutionRuntimeError),
    UnsupportedStatement,
}

impl fmt::Display for SqlDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(err) => write!(f, "{err}"),
            Self::Sql(err) => write!(f, "{err}"),
            Self::Planner(err) => write!(f, "{err}"),
            Self::Runtime(err) => write!(f, "{err}"),
            Self::UnsupportedStatement => {
                write!(f, "statement is not supported by the SQL query driver")
            }
        }
    }
}

impl Error for SqlDriverError {}

impl From<CatalogError> for SqlDriverError {
    fn from(value: CatalogError) -> Self {
        Self::Catalog(value)
    }
}

impl From<SqlError> for SqlDriverError {
    fn from(value: SqlError) -> Self {
        Self::Sql(value)
    }
}

impl From<PlannerError> for SqlDriverError {
    fn from(value: PlannerError) -> Self {
        Self::Planner(value)
    }
}

impl From<ExecutionRuntimeError> for SqlDriverError {
    fn from(value: ExecutionRuntimeError) -> Self {
        Self::Runtime(value)
    }
}

pub struct SqlDriver {
    catalog_service: CatalogService,
    planner: DistributedPlanner,
    runtime: DataFusionExecutionRuntime,
}

impl SqlDriver {
    pub fn new(catalog_service: CatalogService, runtime: DataFusionExecutionRuntime) -> Self {
        Self {
            catalog_service,
            planner: DistributedPlanner::default(),
            runtime,
        }
    }

    pub fn execute(
        &self,
        request: SqlIngressRequest,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let parsed = SqlParser.parse_one(&request.sql)?;
        let bound = SqlBinder.bind(
            parsed,
            &StatementBindingContext {
                session: &request.session,
                request: &request.request,
                catalog_service: &self.catalog_service,
            },
        )?;

        let query_context = QueryContext {
            query_id: request.request.request_id,
        };
        match bound {
            BoundStatement::Plan(statement) => {
                let distributed_plan = self.planner.build(DistributedPlannerRequest {
                    query_context: query_context.clone(),
                    statement,
                })?;
                self.runtime
                    .execute_query(QueryExecutionRequest {
                        query_context,
                        distributed_plan,
                    })
                    .map_err(SqlDriverError::Runtime)
            }
            BoundStatement::Create(statement) => match statement {
                BoundCreateStatement::Database(statement) => CreateDatabaseExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, statement),
                BoundCreateStatement::Table(statement) => CreateTableExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, statement),
            },
            BoundStatement::Drop(statement) => match statement {
                BoundDropStatement::Database(statement) => DropDatabaseExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, statement),
                BoundDropStatement::Table(statement) => DropTableExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, statement),
            },
            BoundStatement::Alter(statement) => match statement {
                BoundAlterStatement::Table(statement) => AlterTableExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, statement),
            },
            BoundStatement::Show(statement) => match statement {
                BoundShowStatement::Catalogs => ShowCatalogsExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context),
                BoundShowStatement::Databases { catalog_name } => ShowDatabasesExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, catalog_name),
                BoundShowStatement::Tables {
                    catalog_name,
                    database_name,
                } => ShowTablesExecutor {
                    catalog_service: &self.catalog_service,
                }
                .execute(query_context, catalog_name, database_name),
            },
            BoundStatement::Set(_)
            | BoundStatement::Transaction(_)
            | BoundStatement::Explain(_) => Err(SqlDriverError::UnsupportedStatement),
        }
    }
}

struct CreateDatabaseExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> CreateDatabaseExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        statement: BoundCreateDatabaseStatement,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let catalog = self.catalog_service.open_catalog(&statement.catalog_name)?;
        catalog.create_database(
            CreateDatabaseRequest::new(statement.database_name).with_options(statement.options),
        )?;
        Ok(QueryExecutionHandle::command(
            query_context,
            "CREATE DATABASE",
        ))
    }
}

struct CreateTableExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> CreateTableExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        statement: BoundCreateTableStatement,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let catalog = self.catalog_service.open_catalog(&statement.catalog_name)?;
        let mut request = CreateTableRequest::new(
            statement.database_name,
            statement.table_name,
            statement.table_schema,
        )
        .with_primary_keys(statement.primary_keys)
        .with_options(statement.table_options);
        if let Some(location) = statement.table_location {
            request = request.with_location(location);
        }
        catalog.create_table(request)?;
        Ok(QueryExecutionHandle::command(query_context, "CREATE TABLE"))
    }
}

struct DropDatabaseExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> DropDatabaseExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        statement: BoundDropDatabaseStatement,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let catalog = self.catalog_service.open_catalog(&statement.catalog_name)?;
        catalog.drop_database(&statement.database_name)?;
        Ok(QueryExecutionHandle::command(
            query_context,
            "DROP DATABASE",
        ))
    }
}

struct DropTableExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> DropTableExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        statement: BoundDropTableStatement,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let table = statement.table;
        let catalog = self.catalog_service.open_catalog(table.path.catalog())?;
        catalog.drop_table(table.path.database(), table.path.table())?;
        Ok(QueryExecutionHandle::command(query_context, "DROP TABLE"))
    }
}

struct AlterTableExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> AlterTableExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        statement: BoundAlterTableStatement,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let table = statement.table;
        let catalog = self.catalog_service.open_catalog(table.path.catalog())?;
        catalog.alter_table(AlterTableRequest::new(
            table.path.database(),
            table.path.table(),
            statement
                .operations
                .iter()
                .map(map_alter_table_operation)
                .collect(),
        ))?;
        Ok(QueryExecutionHandle::command(query_context, "ALTER TABLE"))
    }
}

struct ShowCatalogsExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> ShowCatalogsExecutor<'a> {
    fn execute(&self, query_context: QueryContext) -> Result<QueryExecutionHandle, SqlDriverError> {
        let catalogs = self
            .catalog_service
            .list_catalogs()?
            .into_iter()
            .map(|entry| entry.path.catalog().to_owned())
            .collect::<Vec<_>>();
        build_show_handle(query_context, "SHOW CATALOGS", "catalog_name", catalogs)
    }
}

struct ShowDatabasesExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> ShowDatabasesExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        catalog_name: String,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let databases = self
            .catalog_service
            .list_databases(&catalog_name)?
            .into_iter()
            .map(|entry| entry.path.database().to_owned())
            .collect::<Vec<_>>();
        build_show_handle(query_context, "SHOW DATABASES", "database_name", databases)
    }
}

struct ShowTablesExecutor<'a> {
    catalog_service: &'a CatalogService,
}

impl<'a> ShowTablesExecutor<'a> {
    fn execute(
        &self,
        query_context: QueryContext,
        catalog_name: String,
        database_name: String,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let tables = self
            .catalog_service
            .list_tables(&catalog_name, &database_name)?
            .into_iter()
            .map(|entry| entry.path.table().to_owned())
            .collect::<Vec<_>>();
        build_show_handle(query_context, "SHOW TABLES", "table_name", tables)
    }
}

fn build_show_handle(
    query_context: QueryContext,
    command_tag: impl Into<String>,
    column_name: &str,
    values: Vec<String>,
) -> Result<QueryExecutionHandle, SqlDriverError> {
    let handle = QueryExecutionHandle::query(query_context);
    let mut handle = handle;
    handle.command_tag = command_tag.into();
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        ArrowDataType::Utf8,
        false,
    )]));
    let array: ArrayRef = Arc::new(StringArray::from(values));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|error| {
        SqlDriverError::Runtime(ExecutionRuntimeError::InvalidPlan {
            reason: error.to_string(),
        })
    })?;
    handle.output.push_result(batch)?;
    Ok(handle)
}

fn map_alter_table_operation(operation: &BoundAlterTableOperation) -> AlterTableOperation {
    match operation {
        BoundAlterTableOperation::AddColumn(column) => {
            AlterTableOperation::AddColumn(column.clone())
        }
        BoundAlterTableOperation::DropColumn { column_name } => AlterTableOperation::DropColumn {
            column_name: column_name.clone(),
        },
        BoundAlterTableOperation::RenameColumn { old_name, new_name } => {
            AlterTableOperation::RenameColumn {
                old_name: old_name.clone(),
                new_name: new_name.clone(),
            }
        }
        BoundAlterTableOperation::AlterColumnType {
            column_name,
            data_type,
        } => AlterTableOperation::AlterColumnType {
            column_name: column_name.clone(),
            data_type: data_type.clone(),
        },
        BoundAlterTableOperation::SetTableOption { key, value } => {
            AlterTableOperation::SetTableOption {
                key: key.clone(),
                value: value.clone(),
            }
        }
        BoundAlterTableOperation::RemoveTableOption { key } => {
            AlterTableOperation::RemoveTableOption { key: key.clone() }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, LakeFormatKind,
        open_catalog_store,
    };
    use brewdb_common::config::{ConfigPatch, ConfigScope, global_config_registry};
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use brewdb_sql::{SqlIngressRequest, SqlRequestContext, SqlSessionContext};
    use brewdb_storage::MemoryStorageEngine;
    use uuid::Uuid;

    use super::SqlDriver;
    use crate::execution::DataFusionExecutionRuntime;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("brewdb-sql-driver-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn catalog_service(warehouse: &std::path::Path) -> CatalogService {
        let registry = global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &ConfigPatch::new(ConfigScope::System)
                    .with_entry("brewdb.catalog.store.backend", "memory")
                    .with_entry(
                        "brewdb.catalog.paimon.warehouse",
                        warehouse.to_string_lossy().as_ref(),
                    ),
            )
            .unwrap();
        let service = CatalogService::with_config(
            open_catalog_store(&CatalogConfig {
                store_backend: CatalogStoreBackendKind::Memory,
                paimon_warehouse: warehouse.to_string_lossy().into_owned(),
            }),
            config,
        );
        service
            .create_catalog(CatalogEntry::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
                CatalogMode::Managed,
                LakeFormatKind::Paimon,
            ))
            .unwrap();
        service
    }

    #[test]
    fn sql_driver_executes_ingress_request_through_planner_and_runtime() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let table = catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let storage = Arc::new(MemoryStorageEngine::default());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        storage
            .register_batches(
                &table,
                vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
            )
            .unwrap();

        let query_id = Uuid::new_v4();
        let driver = SqlDriver::new(
            catalog_service,
            DataFusionExecutionRuntime::with_storage(storage),
        );
        let handle = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: query_id,
                },
                sql: "select count(id) from orders".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.query_context.query_id, query_id);
        let batch = handle.output.next_result().unwrap().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
        assert!(handle.output.next_result().unwrap().is_none());
    }

    #[test]
    fn sql_driver_executes_create_table_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DataFusionExecutionRuntime::default(),
        );
        let handle = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "create table orders (id int not null) distributed by (id) into 4 buckets"
                    .to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "CREATE TABLE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        let table = catalog.get_table("sales", "orders").unwrap();
        assert_eq!(
            table.table_options.get("bucket").map(String::as_str),
            Some("4")
        );
        assert_eq!(
            table.table_options.get("bucket-key").map(String::as_str),
            Some("id")
        );
    }

    #[test]
    fn sql_driver_executes_drop_database_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DataFusionExecutionRuntime::default(),
        );
        let handle = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "drop database sales".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "DROP DATABASE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        assert!(catalog.get_database("sales").is_err());
    }

    #[test]
    fn sql_driver_executes_drop_table_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DataFusionExecutionRuntime::default(),
        );
        let handle = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "drop table orders".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "DROP TABLE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        assert!(catalog.get_table("sales", "orders").is_err());
    }

    #[test]
    fn sql_driver_executes_alter_table_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DataFusionExecutionRuntime::default(),
        );
        let handle = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "alter table orders set tblproperties ('owner'='brew')".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "ALTER TABLE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        let table = catalog.get_table("sales", "orders").unwrap();
        assert_eq!(
            table.table_options.get("owner").map(String::as_str),
            Some("brew")
        );
    }

    #[test]
    fn sql_driver_executes_show_statements() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        catalog
            .create_table(CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DataFusionExecutionRuntime::default(),
        );

        let catalogs = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "show catalogs".to_owned(),
                client_capabilities: None,
            })
            .unwrap();
        let catalog_batch = catalogs.output.next_result().unwrap().unwrap();
        let catalog_values = catalog_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(catalog_values.iter().flatten().any(|value| value == "prod"));

        let databases = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "show databases".to_owned(),
                client_capabilities: None,
            })
            .unwrap();
        let database_batch = databases.output.next_result().unwrap().unwrap();
        let database_values = database_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            database_values
                .iter()
                .flatten()
                .any(|value| value == "sales")
        );

        let tables = driver
            .execute(SqlIngressRequest {
                session: SqlSessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "show tables".to_owned(),
                client_capabilities: None,
            })
            .unwrap();
        let table_batch = tables.output.next_result().unwrap().unwrap();
        let table_values = table_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(table_values.iter().flatten().any(|value| value == "orders"));
    }
}
