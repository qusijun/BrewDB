//! SQL-to-runtime driver.

use crate::catalog::{CatalogError, CatalogService};
use crate::common::context::QueryContext;
use crate::common::diagnostics::{DiagnosticError, ErrorCode};
use crate::frontend::ingress::IngressSql;
use crate::parser::ast::Statement;
use crate::parser::dialect::PostgreSqlDialect;
use crate::parser::parser::Parser;
use crate::planner::{
    DistributedFragmentPlanner, FragmentPlanner, LogicalOptimizer, LogicalPlanner,
    LogicalPlanningContext, PlannerError, StandaloneFragmentPlanner,
};
use crate::SqlError;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::runtime::coordinator::QueryCoordinator;
use crate::runtime::execution_graph::{ExecutionRuntimeError, QueryExecutionHandle};

pub(crate) fn sql_to_statement(sql: &str) -> Result<Statement, SqlError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;
    if statements.is_empty() {
        return Err(SqlError::Parse {
            reason: "parser returned no statement".to_string(),
        });
    }
    if statements.len() > 1 {
        return Err(SqlError::UnsupportedStatement {
            reason: "multi-statement SQL is not supported yet".to_string(),
        });
    }
    Ok(statements.remove(0))
}

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

impl DiagnosticError for SqlDriverError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::Catalog(error) => error.error_code(),
            Self::Sql(error) => error.error_code(),
            Self::Planner(error) => error.error_code(),
            Self::Runtime(_) => ErrorCode::INTERNAL,
            Self::UnsupportedStatement => ErrorCode::NOT_IMPLEMENTED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.runtime"
    }
}

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
    logical_optimizer: LogicalOptimizer,
    fragment_planner: Box<dyn FragmentPlanner>,
    coordinator: QueryCoordinator,
}

impl SqlDriver {
    pub fn new(catalog_service: CatalogService, coordinator: QueryCoordinator) -> Self {
        let coordinator = coordinator.with_catalog_service(catalog_service.clone());
        let fragment_planner: Box<dyn FragmentPlanner> = if coordinator.is_single_node() {
            Box::new(StandaloneFragmentPlanner)
        } else {
            Box::new(DistributedFragmentPlanner)
        };
        Self {
            catalog_service,
            logical_planner: LogicalPlanner::default(),
            logical_optimizer: LogicalOptimizer::default(),
            fragment_planner,
            coordinator,
        }
    }

    pub fn execute(&self, request: IngressSql) -> Result<QueryExecutionHandle, SqlDriverError> {
        let parsed = sql_to_statement(&request.sql)?;
        let planned = self.logical_planner.plan(
            parsed,
            &LogicalPlanningContext {
                session: &request.session,
                request: &request.request,
                catalog_service: &self.catalog_service,
            },
        )?;

        let query_context = QueryContext::new(request.request.request_id, request.session.clone())
            .with_settings(request.session.settings.clone());
        if let Some(command) = crate::planner::logical::command::command_plan(&planned) {
            return self
                .coordinator
                .execute_command_plan(query_context, command)
                .map_err(SqlDriverError::Runtime);
        }

        let optimized = self
            .logical_optimizer
            .optimize_with_query_context(planned, &query_context)
            .map_err(|err| PlannerError::InvalidPlan {
                reason: err.to_string(),
            })?;
        let storage = Arc::clone(&self.coordinator.storage);
        let distributed_plan =
            self.fragment_planner
                .plan_fragments(query_context.clone(), optimized, storage)?;
        self.coordinator
            .execute_query(query_context, distributed_plan)
            .map_err(SqlDriverError::Runtime)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::catalog::{
        open_catalog_store, CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, StorageKind,
    };
    use crate::common::config::ConfigSet;
    use crate::common::config::{global_config_registry, ConfigPatch, ConfigScope};
    use crate::common::context::SessionContext;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::frontend::ingress::{IngressSql, SqlRequestContext};
    use crate::storage::MemoryStorageEngine;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use uuid::Uuid;

    use super::{sql_to_statement, SqlDriver};
    use crate::runtime::coordinator::QueryCoordinator;

    #[test]
    fn sql_to_statement_accepts_brewdb_create_table_cluster_by_extension() {
        let statement =
            sql_to_statement("create table t (id int, dt text) cluster by (dt, id)").unwrap();

        match statement {
            crate::parser::ast::Statement::CreateTable(create_table) => {
                assert!(create_table.cluster_by.is_some());
            }
            other => panic!("expected create table, got {other:?}"),
        }
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("brewdb-driver-{}", Uuid::new_v4()));
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
                StorageKind::Paimon,
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
        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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
    fn sql_driver_runs_single_node_queries_without_fragment_exchange() {
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
        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));

        for sql in [
            "select count(id) from orders",
            "select count(id) from orders",
        ] {
            let handle = driver
                .execute(IngressSql {
                    session: SessionContext {
                        session_id: Uuid::new_v4(),
                        user_name: "brew".to_owned(),
                        database_name: Some("sales".to_owned()),
                        catalog_name: Some("prod".to_owned()),
                        settings: ConfigSet::new(),
                    },
                    request: SqlRequestContext {
                        request_id: Uuid::new_v4(),
                    },
                    sql: sql.to_owned(),
                    client_capabilities: None,
                })
                .unwrap();
            let batch = handle.output.next_result().unwrap().unwrap();
            let count = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(count.value(0), 3);
            assert!(handle.output.next_result().unwrap().is_none());
        }
    }

    #[test]
    fn sql_driver_executes_create_table_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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
        assert_eq!(table.table_schema.bucket_keys, vec!["id".to_owned()]);
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
        let storage = Arc::new(MemoryStorageEngine::default());
        storage.register_batches(&table, vec![vec![]]).unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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

        let select = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "select count(id) from orders".to_owned(),
                client_capabilities: None,
            })
            .unwrap();
        let batch = select.output.next_result().unwrap().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 2);
        assert!(select.output.next_result().unwrap().is_none());
    }

    #[test]
    fn sql_driver_executes_copy_from_csv_statement() {
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
        storage.register_batches(&table, vec![vec![]]).unwrap();
        let csv_path = warehouse.path().join("orders.csv");
        fs::write(&csv_path, "id\n1\n2\n3\n").unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: format!(
                    "copy from '{}' to orders with (format csv, header true)",
                    csv_path.display()
                ),
                client_capabilities: None,
            })
            .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
                },
                request: SqlRequestContext {
                    request_id: Uuid::new_v4(),
                },
                sql: "select count(id) from orders".to_owned(),
                client_capabilities: None,
            })
            .unwrap();
        let batch = select.output.next_result().unwrap().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 3);
        assert!(select.output.next_result().unwrap().is_none());
    }

    #[test]
    fn sql_driver_executes_create_database_statement() {
        let warehouse = TestDir::new();
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let error = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());

        let catalogs = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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
        assert!(database_values
            .iter()
            .flatten()
            .any(|value| value == "sales"));

        let tables = driver
            .execute(IngressSql {
                session: SessionContext {
                    session_id: Uuid::new_v4(),
                    user_name: "brew".to_owned(),
                    database_name: Some("sales".to_owned()),
                    catalog_name: Some("prod".to_owned()),
                    settings: ConfigSet::new(),
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
