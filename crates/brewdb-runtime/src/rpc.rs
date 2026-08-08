//! RPC-facing fragment service contracts.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow::record_batch::RecordBatch;
use brewdb_common::runtime::QueryContext;
use brewdb_planner::LocalFragmentPlan;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_expr::Expr;
use datafusion_expr::{LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder, TableType};
use uuid::Uuid;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{Transformed, TreeNode};
use futures::StreamExt;
use tokio::runtime::Runtime;

use crate::exchange::{
    ExchangeBufferManager, ExchangeChannelDescriptor, ExchangeDataPage, ExchangeId,
    route_exchange_batch,
};
use brewdb_execution::FragmentExecutionStatus;
use brewdb_planner::exchange::RemoteSourceNode;

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
        task: FragmentTask,
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
        task: FragmentTask,
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
        task: FragmentTask,
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

#[derive(Clone)]
pub struct FragmentTask {
    pub plan: LocalFragmentPlan,
    pub exchange_inputs: Vec<ExchangeChannelDescriptor>,
    pub exchange_outputs: Vec<ExchangeChannelDescriptor>,
    pub exchange_page_sink: Option<Arc<dyn ExchangePageSink>>,
}

impl FragmentTask {
    pub fn new(plan: LocalFragmentPlan) -> Self {
        Self {
            plan,
            exchange_inputs: Vec::new(),
            exchange_outputs: Vec::new(),
            exchange_page_sink: None,
        }
    }

    pub fn with_exchange_channels(
        mut self,
        exchange_inputs: Vec<ExchangeChannelDescriptor>,
        exchange_outputs: Vec<ExchangeChannelDescriptor>,
    ) -> Self {
        self.exchange_inputs = exchange_inputs;
        self.exchange_outputs = exchange_outputs;
        self
    }

    pub fn with_exchange_page_sink(
        mut self,
        exchange_page_sink: Arc<dyn ExchangePageSink>,
    ) -> Self {
        self.exchange_page_sink = Some(exchange_page_sink);
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

pub struct LocalFragmentService {
    exchange_buffers: Arc<ExchangeBufferManager>,
    tokio_runtime: OnceLock<Runtime>,
}

impl Default for LocalFragmentService {
    fn default() -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            tokio_runtime: OnceLock::new(),
        }
    }
}

impl LocalFragmentService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_exchange_buffer_manager(exchange_buffers: Arc<ExchangeBufferManager>) -> Self {
        Self {
            exchange_buffers,
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
        task: &FragmentTask,
    ) -> Result<(), RpcError> {
        let runtime = self.tokio_runtime()?;
        let session = SessionContext::new();
        let exchange_outputs = task.exchange_outputs.clone();
        let exchange_page_sink = task.exchange_page_sink.clone();
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
        task: &FragmentTask,
    ) -> Result<DataFusionLogicalPlan, RpcError> {
        let exchange_inputs = task
            .exchange_inputs
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
                let rewritten = LogicalPlanBuilder::scan(
                    format!(
                        "__brewdb_fragment_{}",
                        remote_source.source_fragment_ids[0].stage_id.0
                    ),
                    provider_as_source(provider),
                    None,
                )
                .map_err(|err| datafusion_common::DataFusionError::Plan(err.to_string()))?
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

impl FragmentService for LocalFragmentService {
    fn execute_fragment(
        &self,
        _worker_id: Uuid,
        task: FragmentTask,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        let logical_plan =
            self.materialize_exchange_inputs(task.plan.logical_plan.clone(), &task)?;
        self.execute_streaming(task.plan.query_context.clone(), logical_plan, &task)?;
        Ok(FragmentExecutionStatus {
            query_context: task.plan.query_context,
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
            service: Arc::new(LocalFragmentService::default()),
        }
    }
}

impl LocalFragmentTransport {
    pub fn new(service: Arc<dyn FragmentService>) -> Self {
        Self { service }
    }
}

impl FragmentTransport for LocalFragmentTransport {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        task: FragmentTask,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.service.execute_fragment(worker_id, task)
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
        task: FragmentTask,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.transport.execute_fragment(worker_id, task)
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

    use brewdb_common::runtime::QueryContext;
    use brewdb_execution::FragmentExecutionStatus;
    use brewdb_planner::plan::{PlanFragmentId, PlanFragmentKind};
    use brewdb_planner::{LocalFragmentPlan, PlanStageId};
    use datafusion_common::DFSchema;
    use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;

    use crate::exchange::{ExchangeBufferManager, ExchangeDataEncoding, ExchangeDataPage};

    use super::{
        FragmentService, FragmentTask, FragmentTransport, LocalFragmentService,
        LocalFragmentTransport, RpcClient, RpcError, TransportRegistry, TransportRpcClient,
    };

    #[derive(Default)]
    struct RecordingFragmentService {
        calls: Mutex<Vec<(uuid::Uuid, uuid::Uuid)>>,
    }

    impl FragmentService for RecordingFragmentService {
        fn execute_fragment(
            &self,
            worker_id: uuid::Uuid,
            task: FragmentTask,
        ) -> Result<FragmentExecutionStatus, RpcError> {
            self.calls
                .lock()
                .expect("call log lock must not be poisoned")
                .push((worker_id, task.plan.query_context.query_id));
            Ok(FragmentExecutionStatus {
                query_context: task.plan.query_context,
            })
        }

        fn send_exchange_page(&self, _page: ExchangeDataPage) -> Result<(), RpcError> {
            Ok(())
        }

        fn drain_exchange_pages(
            &self,
            _exchange_id: crate::ExchangeId,
        ) -> Result<Vec<ExchangeDataPage>, RpcError> {
            Ok(vec![])
        }
    }

    #[derive(Default)]
    struct RecordingFragmentTransport {
        calls: Mutex<Vec<(uuid::Uuid, uuid::Uuid)>>,
    }

    impl FragmentTransport for RecordingFragmentTransport {
        fn execute_fragment(
            &self,
            worker_id: uuid::Uuid,
            task: FragmentTask,
        ) -> Result<FragmentExecutionStatus, RpcError> {
            self.calls
                .lock()
                .expect("call log lock must not be poisoned")
                .push((worker_id, task.plan.query_context.query_id));
            Ok(FragmentExecutionStatus {
                query_context: task.plan.query_context,
            })
        }

        fn send_exchange_page(&self, _page: ExchangeDataPage) -> Result<(), RpcError> {
            Ok(())
        }

        fn drain_exchange_pages(
            &self,
            _exchange_id: crate::ExchangeId,
        ) -> Result<Vec<ExchangeDataPage>, RpcError> {
            Ok(vec![])
        }
    }

    fn build_plan() -> LocalFragmentPlan {
        let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        LocalFragmentPlan {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragment_id: PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            fragment_kind: PlanFragmentKind::Root,
            logical_plan,
        }
    }

    #[test]
    fn local_fragment_transport_forwards_to_service() {
        let service = Arc::new(RecordingFragmentService::default());
        let transport = LocalFragmentTransport::new(service.clone());
        let worker_id = uuid::Uuid::new_v4();
        let plan = build_plan();
        let query_id = plan.query_context.query_id;

        let result = transport
            .execute_fragment(worker_id, FragmentTask::new(plan))
            .unwrap();

        assert_eq!(result.query_context.query_id, query_id);
        let calls = service.calls.lock().unwrap();
        assert_eq!(calls.as_slice(), &[(worker_id, query_id)]);
    }

    #[test]
    fn transport_rpc_client_forwards_to_transport() {
        let transport = Arc::new(RecordingFragmentTransport::default());
        let client = TransportRpcClient::new(transport.clone());
        let worker_id = uuid::Uuid::new_v4();
        let plan = build_plan();
        let query_id = plan.query_context.query_id;

        let result = client
            .execute_fragment(worker_id, FragmentTask::new(plan))
            .unwrap();

        assert_eq!(result.query_context.query_id, query_id);
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.as_slice(), &[(worker_id, query_id)]);
    }

    #[test]
    fn transport_registry_reports_missing_endpoint() {
        let registry = std::collections::BTreeMap::<String, Arc<dyn FragmentTransport>>::new();
        let result = registry.transport("rpc://missing");
        assert!(matches!(result, Err(RpcError::EndpointNotFound { .. })));
    }

    #[test]
    fn local_fragment_transport_moves_exchange_pages_as_arrow_ipc() {
        let service = Arc::new(LocalFragmentService::with_exchange_buffer_manager(
            Arc::new(ExchangeBufferManager::default()),
        ));
        let transport = LocalFragmentTransport::new(service);
        let page = ExchangeDataPage {
            exchange_id: crate::ExchangeId(7),
            encoding: ExchangeDataEncoding::ArrowIpcStream,
            payload: vec![1, 2, 3],
            end_of_stream: false,
        };

        transport.send_exchange_page(page.clone()).unwrap();
        let drained = transport
            .drain_exchange_pages(crate::ExchangeId(7))
            .unwrap();

        assert_eq!(drained, vec![page]);
    }
}
