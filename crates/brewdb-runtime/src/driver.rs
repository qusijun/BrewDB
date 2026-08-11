//! SQL-to-runtime driver.

use brewdb_catalog::{CatalogError, CatalogService};
use brewdb_common::runtime::QueryContext;
use brewdb_planner::{
    DistributedPlanner, DistributedPlannerRequest, LogicalPlanner, LogicalPlanningContext,
    PlannerError,
};
use brewdb_sql::{SqlError, SqlIngressRequest, SqlParser};
use std::error::Error;
use std::fmt;

use crate::execution::{
    DistributedExecutionRuntime, ExecutionRuntime, ExecutionRuntimeError, QueryExecutionHandle,
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
    logical_planner: LogicalPlanner,
    planner: DistributedPlanner,
    runtime: DistributedExecutionRuntime,
}

impl SqlDriver {
    pub fn new(catalog_service: CatalogService, runtime: DistributedExecutionRuntime) -> Self {
        let runtime = runtime.with_catalog_service(catalog_service.clone());
        Self {
            catalog_service,
            logical_planner: LogicalPlanner::default(),
            planner: DistributedPlanner::default(),
            runtime,
        }
    }

    pub fn execute(
        &self,
        request: SqlIngressRequest,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let parsed = SqlParser.sql_to_statement(&request.sql)?;
        let planned = self.logical_planner.plan(
            parsed,
            &LogicalPlanningContext {
                session: &request.session,
                request: &request.request,
                catalog_service: &self.catalog_service,
            },
        )?;

        let query_context = QueryContext {
            query_id: request.request.request_id,
        };
        let distributed_plan = self.planner.build(DistributedPlannerRequest {
            query_context: query_context.clone(),
            logical_plan: planned,
        })?;
        self.runtime
            .execute_query(QueryExecutionRequest {
                query_context,
                distributed_plan,
            })
            .map_err(SqlDriverError::Runtime)
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
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use brewdb_sql::{SqlIngressRequest, SqlRequestContext, SqlSessionContext};
    use brewdb_storage::MemoryStorageEngine;
    use uuid::Uuid;

    use super::SqlDriver;
    use crate::execution::DistributedExecutionRuntime;

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
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
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
            DistributedExecutionRuntime::with_storage(storage),
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
            DistributedExecutionRuntime::default(),
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
    fn sql_driver_executes_insert_values_statement() {
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
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            ))
            .unwrap();
        let storage = Arc::new(brewdb_storage::MemoryStorageEngine::default());
        storage.register_batches(&table, vec![vec![]]).unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DistributedExecutionRuntime::with_storage(storage),
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
                sql: "insert into orders values (1), (2)".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
    }

    #[test]
    fn sql_driver_executes_create_database_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DistributedExecutionRuntime::default(),
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
                sql: "create database sales".to_owned(),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "CREATE DATABASE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        assert_eq!(
            catalog.get_database("sales").unwrap().path.database(),
            "sales"
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
            DistributedExecutionRuntime::default(),
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
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DistributedExecutionRuntime::default(),
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
    fn sql_driver_rejects_alter_table_until_datafusion_ddl_supports_it() {
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
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DistributedExecutionRuntime::default(),
        );
        let error = driver
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
            .unwrap_err();

        assert!(matches!(error, super::SqlDriverError::Sql(_)));
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
                TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            ))
            .unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            DistributedExecutionRuntime::default(),
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
