//! Runtime-facing execution bridge contracts.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use brewdb_catalog::TableCatalogEntry;
use brewdb_common::runtime::QueryContext;
use brewdb_planner::LocalFragmentPlan;
use brewdb_planner::LogicalPlan as BrewLogicalPlan;
use brewdb_planner::plan::{DistributedPlan, PlanFragment};
use brewdb_storage::StorageEngine;

use crate::exchange::ExchangeChannelDescriptor;
use crate::rpc::{ExchangePageSink, FragmentTask, LocalFragmentTransport, TransportRegistry};
use crate::scheduler::FragmentScheduler;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryExecutionRequest {
    pub query_context: QueryContext,
    pub distributed_plan: DistributedPlan,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryExecutionHandle {
    pub query_context: QueryContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FragmentDispatch {
    pub worker_id: uuid::Uuid,
    pub endpoint: String,
    pub query_context: QueryContext,
    pub fragment: PlanFragment,
    pub table_catalogs: Vec<TableCatalogEntry>,
    pub exchange_inputs: Vec<ExchangeChannelDescriptor>,
    pub exchange_outputs: Vec<ExchangeChannelDescriptor>,
}

struct TransportExchangePageSink {
    transport_registry: Arc<dyn TransportRegistry>,
}

impl ExchangePageSink for TransportExchangePageSink {
    fn send_page(
        &self,
        channel: &ExchangeChannelDescriptor,
        page: crate::exchange::ExchangeDataPage,
    ) -> Result<(), crate::rpc::RpcError> {
        let transport = self
            .transport_registry
            .transport(&channel.target_endpoint)?;
        transport.send_exchange_page(page)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutionRuntimeError {
    InvalidPlan { reason: String },
    RuntimeInitFailed { reason: String },
    StorageError { reason: String },
}

impl fmt::Display for ExecutionRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid execution plan: {reason}"),
            Self::RuntimeInitFailed { reason } => {
                write!(f, "runtime initialization failed: {reason}")
            }
            Self::StorageError { reason } => write!(f, "storage error: {reason}"),
        }
    }
}

impl Error for ExecutionRuntimeError {}

pub trait ExecutionRuntime {
    fn prepare_fragment(
        &self,
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
    ) -> Result<LocalFragmentPlan, ExecutionRuntimeError>;

    fn execute_query(
        &self,
        request: QueryExecutionRequest,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError>;
}

pub struct DataFusionExecutionRuntime {
    scheduler: crate::scheduler::AllAtOnceFragmentScheduler,
    resource_manager: Arc<dyn crate::scheduler::ResourceManager>,
    transport_registry: Arc<dyn TransportRegistry>,
    storage: Arc<dyn StorageEngine>,
}

impl Default for DataFusionExecutionRuntime {
    fn default() -> Self {
        Self {
            scheduler: crate::scheduler::AllAtOnceFragmentScheduler::default(),
            resource_manager: Arc::new(crate::scheduler::StaticResourceManager::new(vec![
                crate::scheduler::WorkerInfo {
                    worker_id: uuid::Uuid::nil(),
                    endpoint: "rpc://worker-0".to_owned(),
                },
            ])),
            transport_registry: Arc::new(BTreeMap::from([(
                "rpc://worker-0".to_owned(),
                Arc::new(LocalFragmentTransport::default())
                    as Arc<dyn crate::rpc::FragmentTransport>,
            )])),
            storage: crate::storage::build_storage_engine(),
        }
    }
}

impl DataFusionExecutionRuntime {
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            ..Self::default()
        }
    }

    pub fn with_resource_manager(
        mut self,
        resource_manager: Arc<dyn crate::scheduler::ResourceManager>,
    ) -> Self {
        self.resource_manager = resource_manager;
        self
    }

    pub fn with_transport_registry(
        mut self,
        transport_registry: Arc<dyn TransportRegistry>,
    ) -> Self {
        self.transport_registry = transport_registry;
        self
    }

    fn local_fragment_tables(plan: &DistributedPlan) -> Vec<TableCatalogEntry> {
        plan.fragments
            .iter()
            .find_map(|fragment| match &fragment.root {
                Some(BrewLogicalPlan::Query(query_root)) => Some(query_root.tables.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }

    fn single_node_worker_id(&self) -> Option<uuid::Uuid> {
        let workers = self.resource_manager.workers();
        match workers.as_slice() {
            [worker] => Some(worker.worker_id),
            _ => None,
        }
    }

    fn execute_single_node_query(
        &self,
        request: QueryExecutionRequest,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        if self.single_node_worker_id().is_none() {
            return Err(ExecutionRuntimeError::InvalidPlan {
                reason: "single-node fast path requires exactly one worker".to_owned(),
            });
        }
        let dispatches = self.build_dispatch_plan(request.clone())?;
        self.execute_dispatches(request, dispatches)
    }

    pub fn build_dispatch_plan(
        &self,
        request: QueryExecutionRequest,
    ) -> Result<Vec<FragmentDispatch>, ExecutionRuntimeError> {
        let table_catalogs = Self::local_fragment_tables(&request.distributed_plan);
        let exchanges = request.distributed_plan.exchanges.clone();
        let schedule = self
            .scheduler
            .schedule(request.distributed_plan, self.resource_manager.as_ref())
            .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                reason: err.to_string(),
            })?;
        let exchange_channels =
            crate::exchange::build_exchange_channels(&exchanges, &schedule.fragments).map_err(
                |err| ExecutionRuntimeError::InvalidPlan {
                    reason: err.to_string(),
                },
            )?;

        Ok(schedule
            .fragments
            .into_iter()
            .map(|scheduled| FragmentDispatch {
                worker_id: scheduled.worker_id,
                endpoint: scheduled.endpoint,
                query_context: scheduled.query_context,
                exchange_inputs: exchange_channels
                    .iter()
                    .filter(|channel| channel.target_fragment_id == scheduled.fragment.fragment_id)
                    .cloned()
                    .collect(),
                exchange_outputs: exchange_channels
                    .iter()
                    .filter(|channel| channel.source_fragment_id == scheduled.fragment.fragment_id)
                    .cloned()
                    .collect(),
                fragment: scheduled.fragment,
                table_catalogs: table_catalogs.clone(),
            })
            .collect())
    }

    pub fn prepare_local_fragment(
        &self,
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
    ) -> Result<LocalFragmentPlan, ExecutionRuntimeError> {
        LocalFragmentPlan::prepare(
            query_context,
            fragment,
            table_catalogs,
            Arc::clone(&self.storage),
        )
        .map_err(|err| ExecutionRuntimeError::InvalidPlan {
            reason: err.to_string(),
        })
    }

    fn execute_dispatches(
        &self,
        request: QueryExecutionRequest,
        dispatches: Vec<FragmentDispatch>,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        std::thread::scope(|scope| {
            let mut joins = Vec::new();
            for dispatch in dispatches {
                let transport_registry = Arc::clone(&self.transport_registry);
                joins.push(scope.spawn(move || {
                    let page_sink = Arc::new(TransportExchangePageSink {
                        transport_registry: Arc::clone(&transport_registry),
                    });
                    let prepared = self.prepare_fragment(
                        dispatch.query_context,
                        dispatch.fragment,
                        dispatch.table_catalogs,
                    )?;
                    let client =
                        transport_registry
                            .transport(&dispatch.endpoint)
                            .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                                reason: err.to_string(),
                            })?;
                    client
                        .execute_fragment(
                            dispatch.worker_id,
                            FragmentTask::new(LocalFragmentPlan {
                                query_context: prepared.query_context.clone(),
                                fragment_id: prepared.fragment_id,
                                fragment_kind: prepared.fragment_kind,
                                logical_plan: prepared.logical_plan,
                            })
                            .with_exchange_channels(
                                dispatch.exchange_inputs.clone(),
                                dispatch.exchange_outputs.clone(),
                            )
                            .with_exchange_page_sink(page_sink),
                        )
                        .map_err(|err| ExecutionRuntimeError::InvalidPlan {
                            reason: err.to_string(),
                        })?;
                    Ok::<_, ExecutionRuntimeError>(())
                }));
            }

            for join in joins {
                join.join()
                    .map_err(|_| ExecutionRuntimeError::RuntimeInitFailed {
                        reason: "fragment dispatch thread panicked".to_owned(),
                    })??;
            }
            Ok::<_, ExecutionRuntimeError>(())
        })?;

        Ok(QueryExecutionHandle {
            query_context: request.query_context,
        })
    }
}

impl ExecutionRuntime for DataFusionExecutionRuntime {
    fn prepare_fragment(
        &self,
        query_context: QueryContext,
        fragment: PlanFragment,
        table_catalogs: Vec<TableCatalogEntry>,
    ) -> Result<LocalFragmentPlan, ExecutionRuntimeError> {
        self.prepare_local_fragment(query_context, fragment, table_catalogs)
    }

    fn execute_query(
        &self,
        request: QueryExecutionRequest,
    ) -> Result<QueryExecutionHandle, ExecutionRuntimeError> {
        if self.single_node_worker_id().is_some() {
            return self.execute_single_node_query(request);
        }
        let dispatches = self.build_dispatch_plan(request.clone())?;
        self.execute_dispatches(request, dispatches)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, LakeFormatKind,
        TableCatalogEntry, TablePath, open_catalog_store,
    };
    use brewdb_common::runtime::QueryContext;
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use brewdb_planner::PlanStageId;
    use brewdb_planner::distributed::{DistributedPlanner, DistributedPlannerRequest};
    use brewdb_planner::exchange::ExchangeNode;
    use brewdb_planner::exchange::RemoteSourceNode;
    use brewdb_planner::logical::{
        LogicalPlan as BrewLogicalPlan, QueryExpression, QueryGroupBy, QueryPlanRoot,
    };
    use brewdb_planner::plan::{DistributedPlan, PlanFragmentId, PlanFragmentKind};
    use brewdb_sql::binder::context::StatementBindingContext;
    use brewdb_sql::statement::{BoundPlanStatement, BoundStatement};
    use brewdb_sql::{SqlBinder, SqlParser, SqlRequestContext, SqlSessionContext};
    use brewdb_storage::MemoryStorageEngine;
    use datafusion_common::DFSchema;
    use datafusion_expr::Expr as DataFusionExpr;
    use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
    use datafusion_expr::{LogicalPlanBuilder, lit};
    use std::collections::BTreeMap;

    use super::{
        DataFusionExecutionRuntime, ExecutionRuntime, ExecutionRuntimeError, PlanFragment,
        QueryExecutionRequest,
    };
    use crate::rpc::{FragmentTransport, LocalFragmentTransport, TransportRegistry};
    use crate::scheduler::{
        FragmentSchedulerError, StaticResourceManager, WorkerInfo, WorkerSelector,
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
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

    #[derive(Clone)]
    struct RecordingForwardingTransport {
        inner: Arc<dyn FragmentTransport>,
        sent_pages: Arc<Mutex<Vec<crate::exchange::ExchangeDataPage>>>,
    }

    impl RecordingForwardingTransport {
        fn new(inner: Arc<dyn FragmentTransport>) -> Self {
            Self {
                inner,
                sent_pages: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl FragmentTransport for RecordingForwardingTransport {
        fn execute_fragment(
            &self,
            worker_id: uuid::Uuid,
            task: crate::rpc::FragmentTask,
        ) -> Result<brewdb_execution::FragmentExecutionStatus, crate::rpc::RpcError> {
            self.inner.execute_fragment(worker_id, task)
        }

        fn send_exchange_page(
            &self,
            page: crate::exchange::ExchangeDataPage,
        ) -> Result<(), crate::rpc::RpcError> {
            self.sent_pages
                .lock()
                .expect("sent page log lock must not be poisoned")
                .push(page.clone());
            self.inner.send_exchange_page(page)
        }

        fn drain_exchange_pages(
            &self,
            exchange_id: crate::exchange::ExchangeId,
        ) -> Result<Vec<crate::exchange::ExchangeDataPage>, crate::rpc::RpcError> {
            self.inner.drain_exchange_pages(exchange_id)
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct SplitWorkerSelector;

    impl WorkerSelector for SplitWorkerSelector {
        fn select_worker(
            &self,
            workers: &[WorkerInfo],
            fragment: &PlanFragment,
        ) -> Result<WorkerInfo, FragmentSchedulerError> {
            match fragment.kind {
                PlanFragmentKind::Source => workers
                    .first()
                    .cloned()
                    .ok_or(FragmentSchedulerError::NoAvailableWorker),
                _ => workers
                    .get(1)
                    .cloned()
                    .or_else(|| workers.first().cloned())
                    .ok_or(FragmentSchedulerError::NoAvailableWorker),
            }
        }
    }

    fn build_fragment() -> PlanFragment {
        let schema = TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]);
        let arrow_schema = schema
            .to_arrow_schema()
            .expect("table schema must convert to arrow");
        let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(arrow_schema).unwrap()),
        });
        PlanFragment {
            fragment_id: PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            kind: PlanFragmentKind::Root,
            root: None,
            local_plan: Some(logical_plan),
        }
    }

    fn build_source_fragment() -> PlanFragment {
        let logical_plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(42i32).alias("answer")])
            .unwrap()
            .build()
            .unwrap();
        PlanFragment {
            fragment_id: PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            kind: PlanFragmentKind::Source,
            root: None,
            local_plan: Some(logical_plan),
        }
    }

    fn build_target_fragment(source_fragment_id: PlanFragmentId) -> PlanFragment {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            ArrowDataType::Int32,
            true,
        )]));
        let schema = Arc::new(DFSchema::try_from(arrow_schema).unwrap());
        PlanFragment {
            fragment_id: PlanFragmentId {
                stage_id: PlanStageId(1),
                fragment_ordinal: 0,
            },
            kind: PlanFragmentKind::Root,
            root: None,
            local_plan: Some(RemoteSourceNode::plan(source_fragment_id, schema)),
        }
    }

    fn build_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            LakeFormatKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn register_table(storage: &MemoryStorageEngine, table: &TableCatalogEntry, values: &[i32]) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        storage.register_batches(table, vec![vec![batch]]).unwrap();
    }

    fn build_table_scan_fragment(table: &TableCatalogEntry) -> PlanFragment {
        let logical_plan = datafusion_expr::LogicalPlanBuilder::scan(
            table.path.table(),
            Arc::new(table.clone()),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        PlanFragment {
            fragment_id: PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            kind: PlanFragmentKind::Root,
            root: Some(BrewLogicalPlan::Query(QueryPlanRoot {
                tables: vec![table.clone()],
                query: QueryExpression {
                    distinct: false,
                    projection: vec![DataFusionExpr::Column(
                        datafusion_common::Column::new_unqualified("id"),
                    )],
                    selection: None,
                    group_by: QueryGroupBy::None,
                    having: None,
                },
                input: Some(logical_plan.clone()),
            })),
            local_plan: Some(logical_plan),
        }
    }

    #[test]
    fn runtime_compiles_fragment_plan_into_datafusion_plan() {
        let fragment = build_fragment();
        let prepared = DataFusionExecutionRuntime::default()
            .prepare_local_fragment(
                QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                fragment,
                vec![],
            )
            .unwrap();
        assert_eq!(prepared.fragment_id.stage_id.0, 0);
        assert_eq!(prepared.fragment_kind, PlanFragmentKind::Root);
    }

    #[test]
    fn runtime_rejects_missing_fragment_plan() {
        let err = DataFusionExecutionRuntime::default()
            .prepare_local_fragment(
                QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                PlanFragment {
                    fragment_id: PlanFragmentId {
                        stage_id: PlanStageId(1),
                        fragment_ordinal: 0,
                    },
                    kind: PlanFragmentKind::Source,
                    root: None,
                    local_plan: None,
                },
                vec![],
            )
            .unwrap_err();
        assert!(matches!(err, ExecutionRuntimeError::InvalidPlan { .. }));
    }

    #[test]
    fn runtime_builds_worker_dispatch_requests_with_table_catalogs() {
        let table = build_table();
        let fragment = build_fragment();
        let distributed_plan = DistributedPlan {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragments: vec![PlanFragment {
                root: Some(BrewLogicalPlan::Query(QueryPlanRoot {
                    tables: vec![table.clone()],
                    query: QueryExpression {
                        distinct: false,
                        projection: vec![DataFusionExpr::Column(
                            datafusion_common::Column::new_unqualified("id"),
                        )],
                        selection: None,
                        group_by: QueryGroupBy::None,
                        having: None,
                    },
                    input: fragment.local_plan.clone(),
                })),
                ..fragment
            }],
            exchanges: vec![],
        };

        let dispatches = DataFusionExecutionRuntime::default()
            .build_dispatch_plan(QueryExecutionRequest {
                query_context: distributed_plan.query_context.clone(),
                distributed_plan,
            })
            .unwrap();

        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].table_catalogs, vec![table]);
        assert_eq!(dispatches[0].worker_id, uuid::Uuid::nil());
        assert!(dispatches[0].exchange_inputs.is_empty());
        assert!(dispatches[0].exchange_outputs.is_empty());
    }

    #[test]
    fn runtime_builds_exchange_channels_for_fragment_dispatches() {
        let root_fragment_id = PlanFragmentId {
            stage_id: PlanStageId(0),
            fragment_ordinal: 0,
        };
        let source_fragment_id = PlanFragmentId {
            stage_id: PlanStageId(1),
            fragment_ordinal: 0,
        };
        let root = PlanFragment {
            fragment_id: root_fragment_id,
            kind: PlanFragmentKind::Root,
            root: None,
            local_plan: Some(build_fragment().local_plan.unwrap()),
        };
        let source = PlanFragment {
            fragment_id: source_fragment_id,
            kind: PlanFragmentKind::Source,
            root: None,
            local_plan: Some(build_fragment().local_plan.unwrap()),
        };
        let distributed_plan = DistributedPlan {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragments: vec![root, source],
            exchanges: vec![ExchangeNode::gather(source_fragment_id, root_fragment_id)],
        };

        let dispatches = DataFusionExecutionRuntime::default()
            .build_dispatch_plan(QueryExecutionRequest {
                query_context: distributed_plan.query_context.clone(),
                distributed_plan,
            })
            .unwrap();

        let root_dispatch = dispatches
            .iter()
            .find(|dispatch| dispatch.fragment.fragment_id == root_fragment_id)
            .expect("root dispatch must exist");
        let source_dispatch = dispatches
            .iter()
            .find(|dispatch| dispatch.fragment.fragment_id == source_fragment_id)
            .expect("source dispatch must exist");
        assert_eq!(root_dispatch.exchange_inputs.len(), 1);
        assert!(root_dispatch.exchange_outputs.is_empty());
        assert!(source_dispatch.exchange_inputs.is_empty());
        assert_eq!(source_dispatch.exchange_outputs.len(), 1);
        assert_eq!(
            root_dispatch.exchange_inputs[0],
            source_dispatch.exchange_outputs[0]
        );
    }

    #[test]
    fn runtime_executes_query_through_single_node_fast_path() {
        let fragment = build_fragment();
        let request = QueryExecutionRequest {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            distributed_plan: DistributedPlan {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                fragments: vec![fragment],
                exchanges: vec![],
            },
        };

        let query_context = request.query_context.clone();
        let handle = DataFusionExecutionRuntime::default()
            .execute_query(request)
            .unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_executes_query_through_transport_registry_when_cluster_has_multiple_workers() {
        let worker_1 = WorkerInfo {
            worker_id: uuid::Uuid::new_v4(),
            endpoint: "rpc://worker-1".to_owned(),
        };
        let worker_2 = WorkerInfo {
            worker_id: uuid::Uuid::new_v4(),
            endpoint: "rpc://worker-2".to_owned(),
        };
        let transport_registry: Arc<dyn TransportRegistry> = Arc::new(BTreeMap::from([
            (
                worker_1.endpoint.clone(),
                Arc::new(LocalFragmentTransport::default()) as Arc<dyn FragmentTransport>,
            ),
            (
                worker_2.endpoint.clone(),
                Arc::new(LocalFragmentTransport::default()) as Arc<dyn FragmentTransport>,
            ),
        ]));
        let runtime = DataFusionExecutionRuntime::default()
            .with_resource_manager(Arc::new(StaticResourceManager::new(vec![
                worker_1.clone(),
                worker_2,
            ])))
            .with_transport_registry(transport_registry);
        let request = QueryExecutionRequest {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            distributed_plan: DistributedPlan {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                fragments: vec![build_fragment()],
                exchanges: vec![],
            },
        };

        let query_context = request.query_context.clone();
        let handle = runtime.execute_query(request).unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_executes_query_against_registered_storage() {
        let table = build_table();
        let storage = Arc::new(MemoryStorageEngine::default());
        register_table(&storage, &table, &[1, 2, 3]);
        let runtime = DataFusionExecutionRuntime::with_storage(storage);
        let request = QueryExecutionRequest {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            distributed_plan: DistributedPlan {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                fragments: vec![build_table_scan_fragment(&table)],
                exchanges: vec![],
            },
        };

        let query_context = request.query_context.clone();
        let handle = runtime.execute_query(request).unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_executes_catalog_bound_sql_end_to_end() {
        let warehouse = TestDir::new("brewdb-runtime-e2e");
        let registry = brewdb_common::config::global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &brewdb_common::config::ConfigPatch::new(
                    brewdb_common::config::ConfigScope::System,
                )
                .with_entry("brewdb.catalog.store.backend", "memory")
                .with_entry(
                    "brewdb.catalog.paimon.warehouse",
                    warehouse.path().to_string_lossy().as_ref(),
                ),
            )
            .unwrap();
        let service = CatalogService::with_config(
            open_catalog_store(&CatalogConfig {
                store_backend: CatalogStoreBackendKind::Memory,
                paimon_warehouse: warehouse.path().to_string_lossy().to_string(),
            }),
            config,
        );
        let entry = CatalogEntry::new(
            uuid::Uuid::new_v4(),
            CatalogPath::new("prod").unwrap(),
            CatalogMode::Managed,
            LakeFormatKind::Paimon,
        );
        service.create_catalog(entry).unwrap();
        let catalog = service.open_catalog("prod").unwrap();
        catalog
            .create_database(CreateDatabaseRequest::new("sales"))
            .unwrap();
        let table = catalog
            .create_table(
                CreateTableRequest::new(
                    "sales",
                    "orders",
                    TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();

        let parser = SqlParser;
        let binder = SqlBinder;
        let parsed = parser.parse_one("select * from orders").unwrap();
        let bound = binder
            .bind(
                parsed,
                &StatementBindingContext {
                    session: &SqlSessionContext {
                        session_id: uuid::Uuid::new_v4(),
                        user_name: "brew".to_owned(),
                        database_name: Some("sales".to_owned()),
                        catalog_name: Some("prod".to_owned()),
                    },
                    request: &SqlRequestContext {
                        request_id: uuid::Uuid::new_v4(),
                    },
                    catalog_service: &service,
                },
            )
            .unwrap();
        let BoundStatement::Plan(BoundPlanStatement::Query(statement)) = bound else {
            panic!("expected query statement");
        };

        let planner = DistributedPlanner::default();
        let plan = planner
            .build(DistributedPlannerRequest {
                query_context: QueryContext {
                    query_id: uuid::Uuid::new_v4(),
                },
                statement: BoundPlanStatement::Query(statement),
            })
            .unwrap();

        let storage = Arc::new(MemoryStorageEngine::default());
        register_table(&storage, &table, &[7, 8, 9]);
        let runtime = DataFusionExecutionRuntime::with_storage(storage);
        let query_context = QueryContext {
            query_id: uuid::Uuid::new_v4(),
        };
        let result = runtime
            .execute_query(QueryExecutionRequest {
                query_context: query_context.clone(),
                distributed_plan: plan,
            })
            .unwrap();

        assert_eq!(result.query_context, query_context);
    }

    #[test]
    fn runtime_streams_exchange_pages_from_source_to_target() {
        let source_fragment = build_source_fragment();
        let source_fragment_id = source_fragment.fragment_id;
        let target_fragment = build_target_fragment(source_fragment_id);
        let distributed_plan = DistributedPlan {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragments: vec![source_fragment, target_fragment],
            exchanges: vec![ExchangeNode::gather(
                source_fragment_id,
                PlanFragmentId {
                    stage_id: PlanStageId(1),
                    fragment_ordinal: 0,
                },
            )],
        };

        let worker_1 = WorkerInfo {
            worker_id: uuid::Uuid::new_v4(),
            endpoint: "rpc://worker-1".to_owned(),
        };
        let worker_2 = WorkerInfo {
            worker_id: uuid::Uuid::new_v4(),
            endpoint: "rpc://worker-2".to_owned(),
        };

        let source_service = Arc::new(
            crate::rpc::LocalFragmentService::with_exchange_buffer_manager(Arc::new(
                crate::exchange::ExchangeBufferManager::default(),
            )),
        );
        let target_service = Arc::new(
            crate::rpc::LocalFragmentService::with_exchange_buffer_manager(Arc::new(
                crate::exchange::ExchangeBufferManager::default(),
            )),
        );
        let target_transport = Arc::new(RecordingForwardingTransport::new(Arc::new(
            LocalFragmentTransport::new(target_service),
        )));
        let sent_pages = Arc::clone(&target_transport.sent_pages);

        let transport_registry: Arc<dyn TransportRegistry> = Arc::new(BTreeMap::from([
            (
                worker_1.endpoint.clone(),
                Arc::new(LocalFragmentTransport::new(source_service)) as Arc<dyn FragmentTransport>,
            ),
            (
                worker_2.endpoint.clone(),
                target_transport.clone() as Arc<dyn FragmentTransport>,
            ),
        ]));
        let runtime = DataFusionExecutionRuntime {
            scheduler: crate::scheduler::AllAtOnceFragmentScheduler {
                worker_selector: Arc::new(SplitWorkerSelector),
            },
            resource_manager: Arc::new(StaticResourceManager::new(vec![
                worker_1.clone(),
                worker_2.clone(),
            ])),
            transport_registry,
            storage: crate::storage::build_storage_engine(),
        };

        let request = QueryExecutionRequest {
            query_context: distributed_plan.query_context.clone(),
            distributed_plan: distributed_plan.clone(),
        };

        let handle = runtime.execute_query(request).unwrap();
        assert_eq!(
            handle.query_context.query_id,
            distributed_plan.query_context.query_id
        );

        let pages = sent_pages.lock().expect("sent page log must be accessible");
        assert_eq!(pages.len(), 2);
        assert!(!pages[0].end_of_stream);
        assert!(pages[1].end_of_stream);
        let batches = pages[0].clone().into_record_batches().unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("target exchange payload must be int32");
        assert_eq!(values.value(0), 42);
    }
}
