//! Runtime execution graph and query output handles.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::common::context::QueryContext;
use crate::planner::distributed::PlanFragment;
use crate::runtime::{ExecutionFragment, FragmentInstance};
use arrow::record_batch::RecordBatch;

use crate::runtime::errors::ExecutionRuntimeError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionGraph {
    pub query_context: QueryContext,
    pub fragments: Vec<ExecutionFragment>,
    pub instances: Vec<FragmentInstance>,
}

impl ExecutionGraph {
    pub fn from_plan_fragments(query_context: QueryContext, fragments: Vec<PlanFragment>) -> Self {
        Self {
            query_context,
            fragments: fragments.into_iter().map(ExecutionFragment::new).collect(),
            instances: vec![],
        }
    }
}

#[derive(Clone)]
pub struct QueryExecutionHandle {
    pub query_context: QueryContext,
    pub command_tag: String,
    pub returns_rows: bool,
    pub output: Arc<QueryOutput>,
}

impl fmt::Debug for QueryExecutionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryExecutionHandle")
            .field("query_context", &self.query_context)
            .field("command_tag", &self.command_tag)
            .field("returns_rows", &self.returns_rows)
            .finish_non_exhaustive()
    }
}

impl QueryExecutionHandle {
    pub fn query(query_context: QueryContext) -> Self {
        Self {
            query_context,
            command_tag: "SELECT".to_owned(),
            returns_rows: true,
            output: Arc::new(QueryOutput::default()),
        }
    }

    pub fn command(query_context: QueryContext, command_tag: impl Into<String>) -> Self {
        Self {
            query_context,
            command_tag: command_tag.into(),
            returns_rows: false,
            output: Arc::new(QueryOutput::default()),
        }
    }
}

#[derive(Default)]
pub struct QueryOutput {
    batches: Mutex<VecDeque<RecordBatch>>,
}

impl QueryOutput {
    pub fn push_result(&self, batch: RecordBatch) -> Result<(), ExecutionRuntimeError> {
        self.batches
            .lock()
            .map_err(|_| ExecutionRuntimeError::RuntimeInitFailed {
                reason: "query result reader lock is poisoned".to_owned(),
            })?
            .push_back(batch);
        Ok(())
    }

    pub fn next_result(&self) -> Result<Option<RecordBatch>, ExecutionRuntimeError> {
        Ok(self
            .batches
            .lock()
            .map_err(|_| ExecutionRuntimeError::RuntimeInitFailed {
                reason: "query result reader lock is poisoned".to_owned(),
            })?
            .pop_front())
    }
}

impl crate::runtime::exchange_service::ResultBatchSink for QueryOutput {
    fn send_batch(&self, batch: RecordBatch) -> Result<(), crate::runtime::RpcError> {
        self.push_result(batch)
            .map_err(|error| crate::runtime::RpcError::ExecutionFailed {
                reason: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::catalog::{
        CatalogConfig, CatalogEntry, CatalogMode, CatalogPath, CatalogService,
        CatalogStoreBackendKind, CreateDatabaseRequest, CreateTableRequest, StorageKind,
        TableCatalogEntry, TablePath, open_catalog_store,
    };
    use crate::common::config::ConfigSet;
    use crate::common::context::QueryContext;
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::planner::CommandTag;
    use crate::planner::distributed::DistributedFragmentPlanner;
    use crate::planner::distributed::exchange::ExchangeNode;
    use crate::planner::distributed::exchange::RemoteSourceNode;
    use crate::planner::distributed::{
        DistributedFragmentPlan, DistributedPlanRoot, FragmentScanSplits, PlanFragmentId,
        PlanFragmentKind,
    };
    use crate::planner::{LocalFragmentPlan, LogicalPlanner, LogicalPlanningContext};
    use crate::runtime::driver::sql_to_statement;
    use crate::storage::memory::MemoryTableEngine;
    use crate::storage::{TableScanSplit, TableScanSplitGroup, open_storage_engine};
    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_common::test_util::TestDir;
    use datafusion_common::DFSchema;
    use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
    use datafusion_expr::{LogicalPlanBuilder, TableSource, TableType, lit};
    use std::collections::BTreeMap;

    use super::PlanFragment;
    use crate::execution::executor::FragmentExecutionEnvelope;
    use crate::runtime::FragmentSchedulerError;
    use crate::runtime::coordinator::QueryCoordinator;
    use crate::runtime::scheduler::{StaticResourceManager, WorkerInfo, WorkerSelector};
    use crate::runtime::transport::{FragmentTransport, LocalFragmentTransport, TransportRegistry};

    #[derive(Clone)]
    struct RecordingForwardingTransport {
        inner: Arc<dyn FragmentTransport>,
        sent_pages: Arc<Mutex<Vec<crate::runtime::exchange::ExchangeDataPage>>>,
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
            envelope: FragmentExecutionEnvelope,
        ) -> Result<crate::execution::FragmentExecutionStatus, crate::runtime::RpcError> {
            self.inner.execute_fragment(worker_id, envelope)
        }

        fn send_exchange_page(
            &self,
            page: crate::runtime::exchange::ExchangeDataPage,
        ) -> Result<(), crate::runtime::RpcError> {
            self.sent_pages
                .lock()
                .expect("sent page log lock must not be poisoned")
                .push(page.clone());
            self.inner.send_exchange_page(page)
        }

        fn drain_exchange_pages(
            &self,
            exchange_id: crate::runtime::exchange::ExchangeId,
        ) -> Result<Vec<crate::runtime::exchange::ExchangeDataPage>, crate::runtime::RpcError>
        {
            self.inner.drain_exchange_pages(exchange_id)
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct SplitWorkerSelector;

    #[derive(Clone, Debug)]
    struct TestTableSource {
        schema: arrow::datatypes::SchemaRef,
    }

    impl TestTableSource {
        fn new(table: &TableCatalogEntry) -> Self {
            Self {
                schema: table
                    .table_schema
                    .to_arrow_schema_ref()
                    .expect("test table schema must convert to arrow"),
            }
        }
    }

    impl TableSource for TestTableSource {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }
    }

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
        let schema = TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]);
        let arrow_schema = schema
            .to_arrow_schema()
            .expect("table schema must convert to arrow");
        let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(arrow_schema).unwrap()),
        });
        PlanFragment {
            fragment_id: PlanFragmentId(0),
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
            fragment_id: PlanFragmentId(0),
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
            fragment_id: PlanFragmentId(1),
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
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
    }

    fn register_table(
        storage: &crate::storage::StorageEngine,
        table: &TableCatalogEntry,
        values: &[i32],
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        storage.register_table_engine(
            table,
            Arc::new(MemoryTableEngine::try_new(table, vec![vec![batch]]).unwrap()),
        );
    }

    fn drain_query_results(handle: &super::QueryExecutionHandle) -> Vec<RecordBatch> {
        let mut batches = Vec::new();
        while let Some(batch) = handle.output.next_result().unwrap() {
            batches.push(batch);
        }
        batches
    }

    fn build_table_scan_fragment(table: &TableCatalogEntry) -> PlanFragment {
        let logical_plan = datafusion_expr::LogicalPlanBuilder::scan(
            table.path.table(),
            Arc::new(TestTableSource::new(table)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        PlanFragment {
            fragment_id: PlanFragmentId(0),
            kind: PlanFragmentKind::Root,
            root: Some(logical_plan.clone()),
            local_plan: Some(logical_plan),
        }
    }

    #[test]
    fn runtime_compiles_fragment_plan_into_datafusion_plan() {
        let fragment = build_fragment();
        let prepared = LocalFragmentPlan::prepare(
            QueryContext::for_test(uuid::Uuid::new_v4()),
            fragment,
            vec![],
            None,
            crate::runtime::storage::build_storage_engine(),
        )
        .unwrap();
        assert_eq!(prepared.fragment_id.0, 0);
        assert_eq!(prepared.fragment_kind, PlanFragmentKind::Root);
    }

    #[test]
    fn runtime_rejects_missing_fragment_plan() {
        let err = LocalFragmentPlan::prepare(
            QueryContext::for_test(uuid::Uuid::new_v4()),
            PlanFragment {
                fragment_id: PlanFragmentId(1),
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: None,
            },
            vec![],
            None,
            crate::runtime::storage::build_storage_engine(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("is missing a local plan"));
    }

    #[test]
    fn runtime_builds_fragment_instances_with_table_catalogs() {
        let table = build_table();
        let fragment = build_fragment();
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![table.clone()],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let instances = QueryCoordinator::default()
            .build_fragment_instances(distributed_plan)
            .unwrap();

        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].table_catalogs, vec![table]);
        assert_eq!(instances[0].worker_id, uuid::Uuid::nil());
        assert!(instances[0].exchange_inputs.is_empty());
        assert!(instances[0].exchange_outputs.is_empty());
    }

    #[test]
    fn runtime_assigns_planned_scan_splits_to_fragment_instance() {
        let fragment_id = PlanFragmentId(0);
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![PlanFragment {
                fragment_id,
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: Some(build_fragment().local_plan.unwrap()),
            }],
            fragment_scan_splits: vec![FragmentScanSplits {
                fragment_id,
                table_scan_splits: TableScanSplitGroup::new(vec![
                    TableScanSplit::new("orders", 0).with_locations(["worker-local-0".to_owned()]),
                    TableScanSplit::new("orders", 1).with_locations(["worker-local-1".to_owned()]),
                ]),
            }],
            exchanges: vec![],
        };

        let instances = QueryCoordinator::default()
            .build_fragment_instances(distributed_plan)
            .unwrap();

        assert_eq!(instances.len(), 2);
        assert_eq!(
            instances[0].table_scan_split.as_ref().unwrap().table_name,
            "orders"
        );
        assert_eq!(instances[1].table_scan_split.as_ref().unwrap().ordinal, 1);
    }

    #[test]
    fn runtime_builds_exchange_channels_for_fragment_instances() {
        let root_fragment_id = PlanFragmentId(0);
        let source_fragment_id = PlanFragmentId(1);
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
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![root, source],
            fragment_scan_splits: vec![],
            exchanges: vec![ExchangeNode::gather(source_fragment_id, root_fragment_id)],
        };

        let instances = QueryCoordinator::default()
            .build_fragment_instances(distributed_plan)
            .unwrap();

        let root_instance = instances
            .iter()
            .find(|instance| instance.fragment_id() == root_fragment_id)
            .expect("root instance must exist");
        let source_instance = instances
            .iter()
            .find(|instance| instance.fragment_id() == source_fragment_id)
            .expect("source instance must exist");
        assert_eq!(root_instance.exchange_inputs.len(), 1);
        assert!(root_instance.exchange_outputs.is_empty());
        assert!(source_instance.exchange_inputs.is_empty());
        assert_eq!(source_instance.exchange_outputs.len(), 1);
        assert_eq!(
            root_instance.exchange_inputs[0],
            source_instance.exchange_outputs[0]
        );
    }

    #[test]
    fn runtime_wires_parallel_source_instances_to_root_exchange_inputs() {
        let source_fragment = build_source_fragment();
        let source_fragment_id = source_fragment.fragment_id;
        let target_fragment = build_target_fragment(source_fragment_id);
        let target_fragment_id = target_fragment.fragment_id;
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![source_fragment, target_fragment],
            fragment_scan_splits: vec![FragmentScanSplits {
                fragment_id: source_fragment_id,
                table_scan_splits: TableScanSplitGroup::new(vec![
                    TableScanSplit::new("orders", 0),
                    TableScanSplit::new("orders", 1),
                ]),
            }],
            exchanges: vec![ExchangeNode::gather(source_fragment_id, target_fragment_id)],
        };

        let instances = QueryCoordinator::default()
            .build_fragment_instances(distributed_plan)
            .unwrap();
        let source_instances = instances
            .iter()
            .filter(|instance| instance.fragment_id() == source_fragment_id)
            .collect::<Vec<_>>();
        let root_instance = instances
            .iter()
            .find(|instance| instance.fragment_id() == target_fragment_id)
            .expect("root instance must exist");

        assert_eq!(source_instances.len(), 2);
        assert!(source_instances.iter().all(|instance| {
            instance.table_scan_split.is_some() && instance.exchange_outputs.len() == 1
        }));
        assert_eq!(root_instance.exchange_inputs.len(), 2);
        assert_ne!(
            root_instance.exchange_inputs[0].exchange_id,
            root_instance.exchange_inputs[1].exchange_id
        );
    }

    #[test]
    fn runtime_executes_single_node_query_through_unified_fragment_scheduling() {
        let fragment = build_fragment();
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = QueryCoordinator::default()
            .execute_query(query_context.clone(), distributed_plan)
            .unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_dispatches_fragment_instance_without_coordinator_prepare() {
        let fragment = build_fragment();
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: query_context.clone(),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = QueryCoordinator::default()
            .execute_query(query_context.clone(), distributed_plan)
            .unwrap();

        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn single_node_fragment_uses_worker_side_prepare_path() {
        let fragment = build_fragment();
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: query_context.clone(),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = QueryCoordinator::default()
            .execute_query(query_context, distributed_plan)
            .unwrap();

        assert_eq!(handle.command_tag, "SELECT");
        assert!(handle.returns_rows);
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
        let runtime = QueryCoordinator::default()
            .with_resource_manager(Arc::new(StaticResourceManager::new(vec![
                worker_1.clone(),
                worker_2,
            ])))
            .with_transport_registry(transport_registry);
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![build_fragment()],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = runtime
            .execute_query(query_context.clone(), distributed_plan)
            .unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_executes_query_against_registered_storage() {
        let table = build_table();
        let storage = open_storage_engine().unwrap();
        register_table(&storage, &table, &[1, 2, 3]);
        let runtime = QueryCoordinator::with_storage(storage);
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![table.clone()],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![build_table_scan_fragment(&table)],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = runtime
            .execute_query(query_context.clone(), distributed_plan)
            .unwrap();
        assert_eq!(handle.query_context, query_context);
    }

    #[test]
    fn runtime_reads_single_node_query_results_through_fragment_instance() {
        let table = build_table();
        let storage = open_storage_engine().unwrap();
        register_table(&storage, &table, &[1, 2, 3]);
        let runtime = QueryCoordinator::with_storage(storage);
        let query_context = QueryContext::for_test(uuid::Uuid::new_v4());
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![table.clone()],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![build_table_scan_fragment(&table)],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        };

        let handle = runtime
            .execute_query(query_context, distributed_plan)
            .unwrap();
        let batches = drain_query_results(&handle);

        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("query result must be int32");
        assert_eq!(
            (0..values.len())
                .map(|idx| values.value(idx))
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn runtime_reads_single_node_aggregate_results_through_exchange() {
        let warehouse = TestDir::new("brewdb-aggregate");
        let registry = crate::common::config::global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &crate::common::config::ConfigPatch::new(
                    crate::common::config::ConfigScope::System,
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
            StorageKind::Paimon,
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
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();
        let storage = open_storage_engine().unwrap();
        register_table(&storage, &table, &[1, 2, 3]);
        let planner = DistributedFragmentPlanner::default();
        let parsed = sql_to_statement("select count(id) from orders").unwrap();
        let query_context = QueryContext::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "brew",
            Some("sales".to_owned()),
            Some("prod".to_owned()),
            ConfigSet::new(),
        );
        let logical_plan = LogicalPlanner::default()
            .plan(
                parsed,
                &LogicalPlanningContext {
                    query_context: &query_context,
                    catalog_service: &service,
                },
            )
            .unwrap();
        let plan = planner
            .build(query_context.clone(), logical_plan, storage.clone())
            .unwrap();
        assert_eq!(plan.fragments.len(), 2);
        assert_eq!(plan.exchanges.len(), 1);

        let handle = QueryCoordinator::with_storage(storage)
            .execute_query(query_context, plan)
            .unwrap();
        let batches = drain_query_results(&handle);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count result must be int64");
        assert_eq!(counts.value(0), 3);
    }

    #[test]
    fn runtime_executes_catalog_planned_sql_end_to_end() {
        let warehouse = TestDir::new("brewdb-e2e");
        let registry = crate::common::config::global_config_registry().unwrap();
        let mut config = registry.materialize_defaults();
        config
            .apply_patch_with_registry(
                &registry,
                &crate::common::config::ConfigPatch::new(
                    crate::common::config::ConfigScope::System,
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
            StorageKind::Paimon,
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
                    TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
                )
                .with_options([("bucket", "1")]),
            )
            .unwrap();

        let logical_planner = LogicalPlanner::default();
        let parsed = sql_to_statement("select * from orders").unwrap();
        let query_context = QueryContext::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            "brew",
            Some("sales".to_owned()),
            Some("prod".to_owned()),
            ConfigSet::new(),
        );
        let planned = logical_planner
            .plan(
                parsed,
                &LogicalPlanningContext {
                    query_context: &query_context,
                    catalog_service: &service,
                },
            )
            .unwrap();
        let logical_plan = planned;

        let storage = open_storage_engine().unwrap();
        register_table(&storage, &table, &[7, 8, 9]);
        let planner = DistributedFragmentPlanner::default();
        let plan = planner
            .build(query_context.clone(), logical_plan, storage.clone())
            .unwrap();

        let runtime = QueryCoordinator::with_storage(storage);
        let result = runtime.execute_query(query_context.clone(), plan).unwrap();

        assert_eq!(result.query_context, query_context);
    }

    #[test]
    fn runtime_streams_exchange_pages_from_source_to_target() {
        let source_fragment = build_source_fragment();
        let source_fragment_id = source_fragment.fragment_id;
        let target_fragment = build_target_fragment(source_fragment_id);
        let distributed_plan = DistributedFragmentPlan {
            query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: CommandTag::Select,
            returns_rows: true,
            fragments: vec![source_fragment, target_fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![ExchangeNode::gather(source_fragment_id, PlanFragmentId(1))],
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
            crate::execution::executor::LocalFragmentExecutor::with_exchange_buffer_manager(
                Arc::new(crate::runtime::exchange::ExchangeBufferManager::default()),
            ),
        );
        let target_service = Arc::new(
            crate::execution::executor::LocalFragmentExecutor::with_exchange_buffer_manager(
                Arc::new(crate::runtime::exchange::ExchangeBufferManager::default()),
            ),
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
        let runtime = QueryCoordinator::default()
            .with_scheduler(crate::runtime::scheduler::AllAtOnceFragmentScheduler {
                worker_selector: Arc::new(SplitWorkerSelector),
            })
            .with_resource_manager(Arc::new(StaticResourceManager::new(vec![
                worker_1.clone(),
                worker_2.clone(),
            ])))
            .with_transport_registry(transport_registry);

        let handle = runtime
            .execute_query(
                distributed_plan.query_context.clone(),
                distributed_plan.clone(),
            )
            .unwrap();
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
