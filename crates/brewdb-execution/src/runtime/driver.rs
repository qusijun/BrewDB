//! SQL-to-runtime driver.

use crate::catalog::CatalogService;
use crate::common::context::QueryContext;
use crate::parser::ast::Statement;
use crate::parser::dialect::PostgreSqlDialect;
use crate::parser::parser::Parser;
use crate::planner::{
    DistributedFragmentPlanner, FragmentPlanner, LogicalOptimizer, LogicalPlanner,
    LogicalPlanningContext, PlannerError, StandaloneFragmentPlanner,
};
use std::sync::Arc;

use crate::runtime::coordinator::QueryCoordinator;
use crate::runtime::errors::SqlDriverError;
use crate::runtime::execution_graph::QueryExecutionHandle;
use crate::runtime::profile::QueryProfiler;
use tracing::{debug, info};

pub(crate) fn sql_to_statement(sql: &str) -> Result<Statement, SqlDriverError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(SqlDriverError::Parser)?;
    if statements.is_empty() {
        return Err(SqlDriverError::InvalidRequest {
            reason: "parser returned no statement".to_string(),
        });
    }
    if statements.len() > 1 {
        return Err(SqlDriverError::UnsupportedStatement {
            reason: "multi-statement SQL is not supported yet".to_string(),
        });
    }
    Ok(statements.remove(0))
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

    pub fn execute(
        &self,
        sql: impl AsRef<str>,
        query_context: QueryContext,
    ) -> Result<QueryExecutionHandle, SqlDriverError> {
        let mut profiler = QueryProfiler::new(query_context.clone());
        let query_elapsed = profiler.summary_metrics.query_elapsed.clone();
        let mut query_elapsed_timer = query_elapsed.timer();
        let result = (|| -> Result<QueryExecutionHandle, SqlDriverError> {
            let parsed = sql_to_statement(sql.as_ref())?;
            let planned = self.logical_planner.plan(
                parsed,
                &LogicalPlanningContext {
                    query_context: &query_context,
                    catalog_service: &self.catalog_service,
                },
            )?;

            if let Some(command) = crate::planner::logical::command::command_plan(&planned) {
                return self
                    .coordinator
                    .execute_command_plan(query_context.clone(), command)
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
                .execute_query(query_context.clone(), distributed_plan, &mut profiler)
                .map_err(SqlDriverError::Runtime)
        })();
        query_elapsed_timer.stop();
        match result {
            Ok(handle) => {
                let profile = profiler.finish_success();
                debug!(target: "brewdb.profile", profile = %profile);
                Ok(handle)
            }
            Err(error) => {
                let profile = profiler.finish_error(error.to_string());
                debug!(target: "brewdb.profile", profile = %profile);
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use crate::catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, StorageKind,
        open_catalog_store,
    };
    use crate::common::config::ConfigSet;
    use crate::common::config::{ConfigPatch, ConfigScope, global_config_registry};
    use crate::common::context::QueryContext;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::storage::memory::MemoryTableEngine;
    use crate::storage::{StorageEngine, open_storage_engine};
    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use uuid::Uuid;

    use super::{SqlDriver, sql_to_statement};
    use crate::runtime::coordinator::QueryCoordinator;
    use crate::runtime::execution_graph::QueryExecutionHandle;
    use brewdb_common::test_util::{TestDir, write_parquet_file};

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

    fn register_batches(
        storage: &StorageEngine,
        table: &crate::catalog::TableCatalogEntry,
        batches: Vec<Vec<RecordBatch>>,
    ) {
        storage.register_table_engine(
            table,
            Arc::new(MemoryTableEngine::try_new(table, batches).unwrap()),
        );
    }

    fn parquet_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()
    }

    fn test_query_context(query_id: Uuid) -> QueryContext {
        QueryContext::new(
            query_id,
            Uuid::new_v4(),
            "brew",
            Some("sales".to_owned()),
            Some("prod".to_owned()),
            ConfigSet::new(),
        )
    }

    fn execute_sql(
        driver: &SqlDriver,
        sql: impl Into<String>,
    ) -> Result<QueryExecutionHandle, super::SqlDriverError> {
        driver.execute(sql.into(), test_query_context(Uuid::new_v4()))
    }

    #[derive(Clone, Default)]
    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn sql_driver_executes_query_context_through_planner_and_runtime() {
        let warehouse = TestDir::new("brewdb-driver");
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

        let storage = open_storage_engine().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        register_batches(
            &storage,
            &table,
            vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
        );

        let query_id = Uuid::new_v4();
        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));
        let handle = driver
            .execute("select count(id) from orders", test_query_context(query_id))
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
    fn sql_driver_emits_readable_profile() {
        let warehouse = TestDir::new("brewdb-driver");
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

        let storage = open_storage_engine().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        register_batches(
            &storage,
            &table,
            vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
        );

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .compact()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer({
                let buffer = buffer.clone();
                move || BufferWriter(buffer.clone())
            })
            .finish();

        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));
        let query_context = test_query_context(Uuid::new_v4());

        tracing::subscriber::with_default(subscriber, || {
            let _ = driver.execute("select count(id) from orders", query_context);
        });

        let output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        assert!(output.contains("QueryProfiler"));
        assert!(output.contains("summary_metrics=["));
        assert!(output.contains("query_elapsed="));
        assert!(!output.contains("profile=\\\"{"));
    }

    #[test]
    fn sql_driver_runs_single_node_queries_without_fragment_exchange() {
        let warehouse = TestDir::new("brewdb-driver");
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

        let storage = open_storage_engine().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        register_batches(
            &storage,
            &table,
            vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
        );
        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));

        for sql in [
            "select count(id) from orders",
            "select count(id) from orders",
        ] {
            let handle = execute_sql(&driver, sql).unwrap();
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
    fn sql_driver_explain_includes_physical_plan() {
        let warehouse = TestDir::new("brewdb-driver");
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

        let storage = open_storage_engine().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        register_batches(
            &storage,
            &table,
            vec![vec![RecordBatch::try_new(schema, vec![values]).unwrap()]],
        );
        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));

        let handle = execute_sql(&driver, "explain select count(id) from orders").unwrap();

        assert_eq!(handle.command_tag, "EXPLAIN");
        assert!(handle.returns_rows);
        let batch = handle.output.next_result().unwrap().unwrap();
        let plan_types = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let plans = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = (0..batch.num_rows())
            .map(|row| (plan_types.value(row), plans.value(row)))
            .collect::<Vec<_>>();

        assert!(
            rows.iter()
                .any(|(plan_type, _)| *plan_type == "logical_plan")
        );
        assert!(
            rows.iter()
                .any(|(plan_type, _)| *plan_type == "physical_plan")
        );
        assert!(
            rows.iter()
                .any(|(_, plan)| plan.contains("AggregateExec") || plan.contains("Aggregate"))
        );
        assert!(handle.output.next_result().unwrap().is_none());
    }

    #[test]
    fn sql_driver_explain_copy_from_includes_sink_physical_plan() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        let csv_path = warehouse.path().join("orders.csv");
        fs::write(&csv_path, "id\n1\n2\n3\n").unwrap();

        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));
        let handle = execute_sql(
            &driver,
            format!(
                "explain copy from '{}' to orders with (format csv, header true)",
                csv_path.display()
            ),
        )
        .unwrap();

        assert_eq!(handle.command_tag, "EXPLAIN");
        assert!(handle.returns_rows);
        let batch = handle.output.next_result().unwrap().unwrap();
        let plan_types = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let plans = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = (0..batch.num_rows())
            .map(|row| (plan_types.value(row), plans.value(row)))
            .collect::<Vec<_>>();

        assert!(
            rows.iter()
                .any(|(plan_type, _)| *plan_type == "physical_plan")
        );
        assert!(rows.iter().any(|(_, plan)| plan.contains("PaimonSinkExec")));
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
        let batch = select.output.next_result().unwrap().unwrap();
        let count = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.value(0), 0);
    }

    #[test]
    fn sql_driver_executes_create_table_statement() {
        let warehouse = TestDir::new("brewdb-driver");
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = execute_sql(
            &driver,
            "create table orders (id int not null) distributed by (id) into 4 buckets",
        )
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
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        register_batches(&storage, &table, vec![vec![]]);

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = execute_sql(&driver, "insert into orders values (1), (2)").unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
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
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        register_batches(&storage, &table, vec![vec![]]);
        let csv_path = warehouse.path().join("orders.csv");
        fs::write(&csv_path, "id\n1\n2\n3\n").unwrap();

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = execute_sql(
            &driver,
            format!(
                "copy from '{}' to orders with (format csv, header true)",
                csv_path.display()
            ),
        )
        .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
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
    fn sql_driver_executes_copy_from_csv_into_paimon_statement() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        let csv_path = warehouse.path().join("orders.csv");
        fs::write(&csv_path, "id\n1\n2\n3\n").unwrap();

        let driver = SqlDriver::new(catalog_service, QueryCoordinator::with_storage(storage));
        let handle = execute_sql(
            &driver,
            format!(
                "copy from '{}' to orders with (format csv, header true)",
                csv_path.display()
            ),
        )
        .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
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
    fn sql_driver_executes_copy_from_parquet_statement_without_format_option() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        register_batches(&storage, &table, vec![vec![]]);
        let parquet_path = warehouse.path().join("orders.parquet");
        write_parquet_file(&parquet_path, parquet_test_batch());

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = execute_sql(
            &driver,
            format!("copy from '{}' to orders", parquet_path.display()),
        )
        .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
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
    fn sql_driver_executes_copy_from_parquet_statement_with_explicit_format() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let storage = open_storage_engine().unwrap();
        register_batches(&storage, &table, vec![vec![]]);
        let parquet_path = warehouse.path().join("orders.data");
        write_parquet_file(&parquet_path, parquet_test_batch());

        let driver = SqlDriver::new(
            catalog_service.clone(),
            QueryCoordinator::with_storage(storage),
        );
        let handle = execute_sql(
            &driver,
            format!(
                "copy from '{}' to orders with (format parquet)",
                parquet_path.display()
            ),
        )
        .unwrap();

        assert_eq!(handle.command_tag, "INSERT");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());

        let select = execute_sql(&driver, "select count(id) from orders").unwrap();
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
        let warehouse = TestDir::new("brewdb-driver");
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = execute_sql(&driver, "create database sales").unwrap();

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
        let warehouse = TestDir::new("brewdb-driver");
        let catalog_service = catalog_service(warehouse.path());
        let catalog = catalog_service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();

        let driver = SqlDriver::new(catalog_service.clone(), QueryCoordinator::default());
        let handle = execute_sql(&driver, "drop database sales").unwrap();

        assert_eq!(handle.command_tag, "DROP DATABASE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        assert!(catalog.get_database("sales").is_err());
    }

    #[test]
    fn sql_driver_executes_drop_table_statement() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let handle = execute_sql(&driver, "drop table orders").unwrap();

        assert_eq!(handle.command_tag, "DROP TABLE");
        assert!(!handle.returns_rows);
        assert!(handle.output.next_result().unwrap().is_none());
        assert!(catalog.get_table("sales", "orders").is_err());
    }

    #[test]
    fn sql_driver_rejects_alter_table_until_datafusion_ddl_supports_it() {
        let warehouse = TestDir::new("brewdb-driver");
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
        let error = execute_sql(
            &driver,
            "alter table orders set tblproperties ('owner'='brew')",
        )
        .unwrap_err();

        assert!(matches!(error, super::SqlDriverError::Planner(_)));
    }

    #[test]
    fn sql_driver_executes_show_statements() {
        let warehouse = TestDir::new("brewdb-driver");
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

        let catalogs = execute_sql(&driver, "show catalogs").unwrap();
        let catalog_batch = catalogs.output.next_result().unwrap().unwrap();
        let catalog_values = catalog_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(catalog_values.iter().flatten().any(|value| value == "prod"));

        let databases = execute_sql(&driver, "show databases").unwrap();
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

        let tables = execute_sql(&driver, "show tables").unwrap();
        let table_batch = tables.output.next_result().unwrap().unwrap();
        let table_values = table_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(table_values.iter().flatten().any(|value| value == "orders"));
    }
}
