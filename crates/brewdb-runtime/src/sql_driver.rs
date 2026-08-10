//! SQL-to-runtime driver.

use std::error::Error;
use std::fmt;

use brewdb_catalog::CatalogService;
use brewdb_common::runtime::QueryContext;
use brewdb_planner::{DistributedPlanner, DistributedPlannerRequest, PlannerError};
use brewdb_sql::{
    BoundStatement, SqlBinder, SqlError, SqlIngressRequest, SqlParser,
    binder::context::StatementBindingContext,
};

use crate::execution::{
    DataFusionExecutionRuntime, ExecutionRuntime, ExecutionRuntimeError, QueryExecutionHandle,
    QueryExecutionRequest,
};

#[derive(Debug)]
pub enum SqlDriverError {
    Sql(SqlError),
    Planner(PlannerError),
    Runtime(ExecutionRuntimeError),
    UnsupportedStatement,
}

impl fmt::Display for SqlDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
        let BoundStatement::Plan(statement) = bound else {
            return Err(SqlDriverError::UnsupportedStatement);
        };

        let query_context = QueryContext {
            query_id: request.request.request_id,
        };
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
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
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
}
