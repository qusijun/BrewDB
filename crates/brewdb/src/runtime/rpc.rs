//! RPC-facing fragment service contracts.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::common::runtime::QueryContext;
use crate::planner::LocalFragmentPlan;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_common::TableReference;
use datafusion_expr::Expr;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder, TableType};
use uuid::Uuid;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{Transformed, TreeNode};
use futures::StreamExt;
use tokio::runtime::Runtime;

use crate::execution::FragmentExecutionStatus;
use crate::planner::distributed::exchange::RemoteSourceNode;
use crate::runtime::exchange::{
    route_exchange_batch, ExchangeBufferManager, ExchangeChannelDescriptor, ExchangeDataPage,
    ExchangeId,
};
use crate::runtime::execution_graph::FragmentInstance;
use crate::storage::StorageEngine;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RpcError {
    EndpointNotFound { endpoint: String },
    ExecutionFailed { reason: String },
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndpointNotFound { endpoint } => write!(f, "rpc endpoint not found: {endpoint}"),
            Self::ExecutionFailed { reason } => write!(f, "rpc execution failed: {reason}"),
        }
    }
}

impl Error for RpcError {}

pub trait RpcClient: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError>;
}

pub trait FragmentTransport: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError>;
}

pub trait TransportRegistry: Send + Sync {
    fn transport(&self, endpoint: &str) -> Result<Arc<dyn FragmentTransport>, RpcError>;
}

pub trait FragmentService: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError>;
}

pub trait ExchangePageSink: Send + Sync {
    fn send_page(
        &self,
        channel: &ExchangeChannelDescriptor,
        page: ExchangeDataPage,
    ) -> Result<(), RpcError>;
}

pub trait ResultBatchSink: Send + Sync {
    fn send_batch(&self, batch: RecordBatch) -> Result<(), RpcError>;
}

#[derive(Clone)]
pub struct FragmentExecutionEnvelope {
    pub instance: FragmentInstance,
    pub exchange_page_sink: Option<Arc<dyn ExchangePageSink>>,
    pub result_batch_sink: Option<Arc<dyn ResultBatchSink>>,
}

impl FragmentExecutionEnvelope {
    pub fn new(instance: FragmentInstance) -> Self {
        Self {
            instance,
            exchange_page_sink: None,
            result_batch_sink: None,
        }
    }

    pub fn with_exchange_page_sink(
        mut self,
        exchange_page_sink: Arc<dyn ExchangePageSink>,
    ) -> Self {
        self.exchange_page_sink = Some(exchange_page_sink);
        self
    }

    pub fn with_result_batch_sink(mut self, result_batch_sink: Arc<dyn ResultBatchSink>) -> Self {
        self.result_batch_sink = Some(result_batch_sink);
        self
    }
}

struct ExchangePartitionStream {
    exchange_id: ExchangeId,
    schema: datafusion::arrow::datatypes::SchemaRef,
    exchange_buffers: Arc<ExchangeBufferManager>,
}

impl fmt::Debug for ExchangePartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangePartitionStream")
            .field("exchange_id", &self.exchange_id)
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for ExchangePartitionStream {
    fn schema(&self) -> &datafusion::arrow::datatypes::SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self
            .exchange_buffers
            .take_receiver(self.exchange_id)
            .expect("exchange receiver must exist");
        let stream = futures::stream::unfold(
            (receiver, VecDeque::<RecordBatch>::new()),
            move |(mut receiver, mut pending)| async move {
                loop {
                    if let Some(batch) = pending.pop_front() {
                        return Some((Ok(batch), (receiver, pending)));
                    }
                    let page = receiver.recv().await?;
                    if page.end_of_stream {
                        return None;
                    }
                    let batches = match page.into_record_batches() {
                        Ok(batches) => batches,
                        Err(err) => {
                            return Some((
                                Err(datafusion_common::DataFusionError::Plan(err.to_string())),
                                (receiver, pending),
                            ));
                        }
                    };
                    pending.extend(batches.into_iter());
                }
            },
        );
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

struct ExchangeStreamTableProvider {
    schema: datafusion::arrow::datatypes::SchemaRef,
    exchange_ids: Vec<ExchangeId>,
    exchange_buffers: Arc<ExchangeBufferManager>,
}

impl fmt::Debug for ExchangeStreamTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeStreamTableProvider")
            .field("schema", &self.schema)
            .field("exchange_ids", &self.exchange_ids)
            .finish()
    }
}

#[async_trait]
impl TableProvider for ExchangeStreamTableProvider {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let partitions = self
            .exchange_ids
            .iter()
            .map(|exchange_id| {
                Arc::new(ExchangePartitionStream {
                    exchange_id: *exchange_id,
                    schema: Arc::clone(&self.schema),
                    exchange_buffers: Arc::clone(&self.exchange_buffers),
                }) as Arc<dyn PartitionStream>
            })
            .collect::<Vec<_>>();
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            partitions,
            projection,
            Vec::<datafusion::physical_expr::LexOrdering>::new(),
            false,
            limit,
        )?))
    }
}

impl TransportRegistry for BTreeMap<String, Arc<dyn FragmentTransport>> {
    fn transport(&self, endpoint: &str) -> Result<Arc<dyn FragmentTransport>, RpcError> {
        self.get(endpoint)
            .cloned()
            .ok_or_else(|| RpcError::EndpointNotFound {
                endpoint: endpoint.to_owned(),
            })
    }
}

pub struct LocalFragmentExecutor {
    exchange_buffers: Arc<ExchangeBufferManager>,
    session: SessionContext,
    storage: Arc<dyn StorageEngine>,
    tokio_runtime: OnceLock<Runtime>,
}

impl Default for LocalFragmentExecutor {
    fn default() -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            session: open_session(),
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }
}

impl LocalFragmentExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            session: open_session(),
            storage,
            tokio_runtime: OnceLock::new(),
        }
    }

    pub fn with_exchange_buffer_manager(exchange_buffers: Arc<ExchangeBufferManager>) -> Self {
        Self {
            exchange_buffers,
            session: open_session(),
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }

    fn tokio_runtime(&self) -> Result<&Runtime, RpcError> {
        self.tokio_runtime
            .get_or_init(|| Runtime::new().expect("tokio runtime must build"));
        self.tokio_runtime
            .get()
            .ok_or_else(|| RpcError::ExecutionFailed {
                reason: "tokio runtime was not initialized".to_owned(),
            })
    }

    fn execute_streaming(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        envelope: &FragmentExecutionEnvelope,
    ) -> Result<(), RpcError> {
        let runtime = self.tokio_runtime()?;
        let session = self.session.clone();
        let exchange_outputs = envelope.instance.exchange_outputs.clone();
        let exchange_page_sink = envelope.exchange_page_sink.clone();
        let result_batch_sink = envelope.result_batch_sink.clone();
        let exchange_buffers = Arc::clone(&self.exchange_buffers);
        runtime.block_on(async move {
            let state = session.state();
            let task_ctx = session.task_ctx();
            let physical_plan = state
                .create_physical_plan(&logical_plan)
                .await
                .map_err(|err| RpcError::ExecutionFailed {
                    reason: err.to_string(),
                })?;
            let mut stream = execute_stream(physical_plan, task_ctx).map_err(|err| {
                RpcError::ExecutionFailed {
                    reason: err.to_string(),
                }
            })?;
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|err| RpcError::ExecutionFailed {
                    reason: err.to_string(),
                })?;
                if exchange_outputs.is_empty() {
                    if let Some(sink) = &result_batch_sink {
                        sink.send_batch(batch)?;
                    }
                    continue;
                }
                for (channel, routed_batch) in route_exchange_batch(&exchange_outputs, batch)
                    .map_err(|err| RpcError::ExecutionFailed {
                        reason: err.to_string(),
                    })?
                {
                    let page =
                        ExchangeDataPage::from_record_batch(channel.exchange_id, routed_batch)
                            .map_err(|err| RpcError::ExecutionFailed {
                                reason: err.to_string(),
                            })?;
                    if let Some(sink) = &exchange_page_sink {
                        sink.send_page(&channel, page)?;
                    } else {
                        exchange_buffers.enqueue_page(page).map_err(|err| {
                            RpcError::ExecutionFailed {
                                reason: err.to_string(),
                            }
                        })?;
                    }
                }
            }
            for channel in &exchange_outputs {
                let page = ExchangeDataPage::end_of_stream(channel.exchange_id);
                if let Some(sink) = &exchange_page_sink {
                    sink.send_page(channel, page)?;
                } else {
                    exchange_buffers.enqueue_page(page).map_err(|err| {
                        RpcError::ExecutionFailed {
                            reason: err.to_string(),
                        }
                    })?;
                }
            }
            Ok(())
        })?;
        let _ = query_context;
        Ok(())
    }

    fn materialize_exchange_inputs(
        &self,
        plan: DataFusionLogicalPlan,
        exchange_inputs: &[ExchangeChannelDescriptor],
    ) -> Result<DataFusionLogicalPlan, RpcError> {
        let exchange_inputs = exchange_inputs
            .iter()
            .map(|channel| (channel.source_fragment_id, channel.clone()))
            .collect::<HashMap<_, _>>();

        plan.transform_down(|node| match &node {
            DataFusionLogicalPlan::Extension(extension) => {
                let Some(remote_source) =
                    extension.node.as_any().downcast_ref::<RemoteSourceNode>()
                else {
                    return Ok(Transformed::no(node));
                };
                let exchange_ids = remote_source
                    .source_fragment_ids
                    .iter()
                    .map(|source_fragment_id| {
                        exchange_inputs
                            .get(source_fragment_id)
                            .map(|channel| channel.exchange_id)
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Plan(format!(
                                    "exchange input for fragment {:?} is missing",
                                    source_fragment_id
                                ))
                            })
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let provider = Arc::new(ExchangeStreamTableProvider {
                    schema: Arc::new(remote_source.schema.as_arrow().clone()),
                    exchange_ids,
                    exchange_buffers: Arc::clone(&self.exchange_buffers),
                });
                let mut builder = LogicalPlanBuilder::scan(
                    format!(
                        "__brewdb_fragment_{}",
                        remote_source.source_fragment_ids[0].0
                    ),
                    provider_as_source(provider),
                    None,
                )
                .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?;
                if let Some(qualifier) = remote_source_single_qualifier(remote_source) {
                    builder = builder
                        .alias(qualifier)
                        .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?;
                }
                let rewritten = builder
                    .build()
                    .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?;
                Ok(Transformed::yes(rewritten))
            }
            _ => Ok(Transformed::no(node)),
        })
        .map(|result| result.data)
        .map_err(|err| RpcError::ExecutionFailed {
            reason: err.to_string(),
        })
    }
}

fn open_session() -> SessionContext {
    let session = SessionContext::new();
    crate::runtime::function::register_internal_functions(&session);
    session
}

fn remote_source_single_qualifier(remote_source: &RemoteSourceNode) -> Option<TableReference> {
    let mut qualifier = None;
    for (field_qualifier, _) in remote_source.schema.iter() {
        let Some(field_qualifier) = field_qualifier else {
            continue;
        };
        match &qualifier {
            Some(existing) if existing != field_qualifier => return None,
            Some(_) => {}
            None => qualifier = Some(field_qualifier.clone()),
        }
    }
    qualifier
}

impl FragmentService for LocalFragmentExecutor {
    fn execute_fragment(
        &self,
        _worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        let FragmentExecutionEnvelope { instance, .. } = &envelope;
        let prepared = LocalFragmentPlan::prepare(
            instance.query_context.clone(),
            instance.execution_fragment.fragment.clone(),
            instance.table_catalogs.clone(),
            instance.table_scan_splits.clone(),
            Arc::clone(&self.storage),
        )
        .map_err(|err| RpcError::ExecutionFailed {
            reason: err.to_string(),
        })?;
        let logical_plan = self.materialize_exchange_inputs(
            prepared.logical_plan.clone(),
            instance.exchange_inputs.as_slice(),
        )?;
        self.execute_streaming(prepared.query_context.clone(), logical_plan, &envelope)?;
        Ok(FragmentExecutionStatus {
            query_context: prepared.query_context,
        })
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError> {
        self.exchange_buffers
            .enqueue_page(page)
            .map_err(|err| RpcError::ExecutionFailed {
                reason: err.to_string(),
            })
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError> {
        self.exchange_buffers
            .drain_pages_by_id(exchange_id)
            .map_err(|err| RpcError::ExecutionFailed {
                reason: err.to_string(),
            })
    }
}

pub struct LocalFragmentTransport {
    service: Arc<dyn FragmentService>,
}

impl Default for LocalFragmentTransport {
    fn default() -> Self {
        Self {
            service: Arc::new(LocalFragmentExecutor::default()),
        }
    }
}

impl LocalFragmentTransport {
    pub fn new(service: Arc<dyn FragmentService>) -> Self {
        Self { service }
    }

    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            service: Arc::new(LocalFragmentExecutor::with_storage(storage)),
        }
    }
}

impl FragmentTransport for LocalFragmentTransport {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.service.execute_fragment(worker_id, envelope)
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError> {
        self.service.send_exchange_page(page)
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError> {
        self.service.drain_exchange_pages(exchange_id)
    }
}

pub struct TransportRpcClient {
    transport: Arc<dyn FragmentTransport>,
}

impl TransportRpcClient {
    pub fn new(transport: Arc<dyn FragmentTransport>) -> Self {
        Self { transport }
    }
}

impl RpcClient for TransportRpcClient {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.transport.execute_fragment(worker_id, envelope)
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError> {
        self.transport.send_exchange_page(page)
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError> {
        self.transport.drain_exchange_pages(exchange_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::common::runtime::QueryContext;
    use crate::execution::FragmentExecutionStatus;
    use crate::planner::distributed::plan::{PlanFragment, PlanFragmentId, PlanFragmentKind};
    use crate::planner::distributed::split::TableScanSplitGroup;
    use datafusion_common::DFSchema;
    use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;

    use crate::runtime::exchange::{ExchangeBufferManager, ExchangeDataEncoding, ExchangeDataPage};
    use crate::runtime::execution_graph::{ExecutionFragment, FragmentInstance, QueryOutput};

    use super::{
        FragmentExecutionEnvelope, FragmentService, FragmentTransport, LocalFragmentExecutor,
        LocalFragmentTransport, RpcClient, RpcError, TransportRegistry, TransportRpcClient,
    };

    #[derive(Default)]
    struct RecordingFragmentService {
        received_instances: Mutex<Vec<uuid::Uuid>>,
    }

    impl FragmentService for RecordingFragmentService {
        fn execute_fragment(
            &self,
            _worker_id: uuid::Uuid,
            envelope: FragmentExecutionEnvelope,
        ) -> Result<FragmentExecutionStatus, RpcError> {
            self.received_instances
                .lock()
                .expect("received instance log lock must not be poisoned")
                .push(envelope.instance.instance_id);
            Ok(FragmentExecutionStatus {
                query_context: envelope.instance.query_context,
            })
        }

        fn send_exchange_page(&self, _page: ExchangeDataPage) -> Result<(), RpcError> {
            Ok(())
        }

        fn drain_exchange_pages(
            &self,
            _exchange_id: crate::runtime::ExchangeId,
        ) -> Result<Vec<ExchangeDataPage>, RpcError> {
            Ok(vec![])
        }
    }

    #[derive(Default)]
    struct RecordingFragmentTransport {
        received_instances: Mutex<Vec<uuid::Uuid>>,
    }

    impl FragmentTransport for RecordingFragmentTransport {
        fn execute_fragment(
            &self,
            _worker_id: uuid::Uuid,
            envelope: FragmentExecutionEnvelope,
        ) -> Result<FragmentExecutionStatus, RpcError> {
            self.received_instances
                .lock()
                .expect("received instance log lock must not be poisoned")
                .push(envelope.instance.instance_id);
            Ok(FragmentExecutionStatus {
                query_context: envelope.instance.query_context,
            })
        }

        fn send_exchange_page(&self, _page: ExchangeDataPage) -> Result<(), RpcError> {
            Ok(())
        }

        fn drain_exchange_pages(
            &self,
            _exchange_id: crate::runtime::ExchangeId,
        ) -> Result<Vec<ExchangeDataPage>, RpcError> {
            Ok(vec![])
        }
    }

    fn build_instance() -> FragmentInstance {
        let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let mut instance = FragmentInstance::scheduled(
            uuid::Uuid::new_v4(),
            ExecutionFragment::new(PlanFragment {
                fragment_id: PlanFragmentId(0),
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: Some(logical_plan),
            }),
            uuid::Uuid::new_v4(),
            "rpc://worker-0",
            TableScanSplitGroup::default(),
        );
        instance.query_context = QueryContext {
            query_id: uuid::Uuid::new_v4(),
        };
        instance
    }

    #[test]
    fn fragment_execution_envelope_keeps_sinks_out_of_fragment_instance() {
        let instance = FragmentInstance::scheduled(
            uuid::Uuid::new_v4(),
            ExecutionFragment::new(PlanFragment {
                fragment_id: PlanFragmentId(7),
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: Some(DataFusionLogicalPlan::EmptyRelation(
                    datafusion_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(DFSchema::empty()),
                    },
                )),
            }),
            uuid::Uuid::new_v4(),
            "rpc://worker-7",
            TableScanSplitGroup::default(),
        );

        let envelope = FragmentExecutionEnvelope::new(instance.clone());

        assert_eq!(envelope.instance.fragment_id(), PlanFragmentId(7));
        assert!(envelope.exchange_page_sink.is_none());
        assert!(envelope.result_batch_sink.is_none());
        assert_eq!(instance.fragment_id(), PlanFragmentId(7));
    }

    #[test]
    fn local_fragment_transport_forwards_to_service() {
        let service = Arc::new(RecordingFragmentService::default());
        let transport = LocalFragmentTransport::new(service.clone());
        let worker_id = uuid::Uuid::new_v4();
        let instance = build_instance();
        let instance_id = instance.instance_id;
        let query_id = instance.query_context.query_id;

        let result = transport
            .execute_fragment(worker_id, FragmentExecutionEnvelope::new(instance))
            .unwrap();

        assert_eq!(result.query_context.query_id, query_id);
        assert_eq!(
            service
                .received_instances
                .lock()
                .expect("received instance log lock must not be poisoned")[0],
            instance_id
        );
    }

    #[test]
    fn transport_rpc_client_forwards_to_transport() {
        let transport = Arc::new(RecordingFragmentTransport::default());
        let client = TransportRpcClient::new(transport.clone());
        let worker_id = uuid::Uuid::new_v4();
        let instance = build_instance();
        let instance_id = instance.instance_id;
        let query_id = instance.query_context.query_id;

        let result = client
            .execute_fragment(worker_id, FragmentExecutionEnvelope::new(instance))
            .unwrap();

        assert_eq!(result.query_context.query_id, query_id);
        assert_eq!(
            transport
                .received_instances
                .lock()
                .expect("received instance log lock must not be poisoned")[0],
            instance_id
        );
    }

    #[test]
    fn transport_registry_reports_missing_endpoint() {
        let registry = std::collections::BTreeMap::<String, Arc<dyn FragmentTransport>>::new();
        let result = registry.transport("rpc://missing");
        assert!(matches!(result, Err(RpcError::EndpointNotFound { .. })));
    }

    #[test]
    fn local_fragment_transport_moves_exchange_pages_as_arrow_ipc() {
        let service = Arc::new(LocalFragmentExecutor::with_exchange_buffer_manager(
            Arc::new(ExchangeBufferManager::default()),
        ));
        let transport = LocalFragmentTransport::new(service);
        let page = ExchangeDataPage {
            exchange_id: crate::runtime::ExchangeId(7),
            encoding: ExchangeDataEncoding::ArrowIpcStream,
            payload: vec![1, 2, 3],
            end_of_stream: false,
        };

        transport.send_exchange_page(page.clone()).unwrap();
        let drained = transport
            .drain_exchange_pages(crate::runtime::ExchangeId(7))
            .unwrap();

        assert_eq!(drained, vec![page]);
    }

    #[test]
    fn local_fragment_executor_prepares_fragment_instance_on_worker() {
        let query_context = QueryContext {
            query_id: uuid::Uuid::new_v4(),
        };
        let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let fragment = PlanFragment {
            fragment_id: PlanFragmentId(3),
            kind: PlanFragmentKind::Root,
            root: None,
            local_plan: Some(logical_plan),
        };
        let mut instance = FragmentInstance::scheduled(
            uuid::Uuid::new_v4(),
            ExecutionFragment::new(fragment),
            uuid::Uuid::new_v4(),
            "rpc://worker-3",
            TableScanSplitGroup::default(),
        );
        instance.query_context = query_context.clone();

        let status = LocalFragmentExecutor::default()
            .execute_fragment(
                uuid::Uuid::new_v4(),
                FragmentExecutionEnvelope::new(instance),
            )
            .unwrap();

        assert_eq!(status.query_context, query_context);
    }

    #[test]
    fn local_fragment_executor_registers_internal_functions_at_open() {
        let service = LocalFragmentExecutor::default();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let logical_plan = runtime.block_on(async {
            service
                .session
                .sql("select paimon_hash(cast(1 as int), cast(4 as int))")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap()
        });
        let mut instance = FragmentInstance::scheduled(
            uuid::Uuid::new_v4(),
            ExecutionFragment::new(PlanFragment {
                fragment_id: PlanFragmentId(0),
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: Some(logical_plan),
            }),
            uuid::Uuid::new_v4(),
            "rpc://worker-0",
            TableScanSplitGroup::default(),
        );
        instance.query_context = QueryContext {
            query_id: uuid::Uuid::new_v4(),
        };
        let output = Arc::new(QueryOutput::default());

        service
            .execute_fragment(
                uuid::Uuid::new_v4(),
                FragmentExecutionEnvelope::new(instance).with_result_batch_sink(output.clone()),
            )
            .unwrap();

        let batch = output.next_result().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
