//! DataFusion-backed fragment executor contracts.

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

use crate::common::context::QueryContext;
use crate::execution::exchange::WorkerExchangeService;
use crate::planner::LocalFragmentPlan;
use crate::runtime::exchange::{
    route_exchange_batch, ExchangeBufferManager, ExchangeDataPage, ExchangeId,
};
use crate::runtime::exchange_service::{ExchangePageSink, ResultBatchSink};
use crate::runtime::execution_graph::FragmentInstance;
use crate::storage::StorageEngine;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execute_stream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_common::error::Result as DataFusionResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::TableReference;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_expr::{LogicalPlanBuilder, TableType};
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FragmentExecutionStatus {
    pub query_context: QueryContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentExecutorError {
    InvalidPlan { reason: String },
    RuntimeInitFailed { reason: String },
}

impl fmt::Display for FragmentExecutorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPlan { reason } => write!(f, "invalid fragment plan: {reason}"),
            Self::RuntimeInitFailed { reason } => {
                write!(f, "runtime initialization failed: {reason}")
            }
        }
    }
}

impl Error for FragmentExecutorError {}

pub trait FragmentExecutor: Send + Sync {
    fn execute_fragment(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
    ) -> Result<FragmentExecutionStatus, FragmentExecutorError>;
}

pub struct DataFusionFragmentExecutor {
    tokio_runtime: OnceLock<Runtime>,
}

impl Default for DataFusionFragmentExecutor {
    fn default() -> Self {
        Self {
            tokio_runtime: OnceLock::new(),
        }
    }
}

impl DataFusionFragmentExecutor {
    fn tokio_runtime(&self) -> Result<&Runtime, FragmentExecutorError> {
        self.tokio_runtime
            .get_or_init(|| Runtime::new().expect("tokio runtime must build"));
        self.tokio_runtime
            .get()
            .ok_or_else(|| FragmentExecutorError::RuntimeInitFailed {
                reason: "tokio runtime was not initialized".to_owned(),
            })
    }
}

impl FragmentExecutor for DataFusionFragmentExecutor {
    fn execute_fragment(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
    ) -> Result<FragmentExecutionStatus, FragmentExecutorError> {
        let runtime = self.tokio_runtime()?;
        let session =
            crate::runtime::datafusion_context::session_context(&query_context).map_err(|err| {
                FragmentExecutorError::InvalidPlan {
                    reason: err.to_string(),
                }
            })?;
        runtime.block_on(async move {
            let df = session
                .execute_logical_plan(logical_plan)
                .await
                .map_err(|err| FragmentExecutorError::InvalidPlan {
                    reason: err.to_string(),
                })?;
            df.collect()
                .await
                .map_err(|err| FragmentExecutorError::InvalidPlan {
                    reason: err.to_string(),
                })?;
            Ok::<_, FragmentExecutorError>(())
        })?;
        Ok(FragmentExecutionStatus { query_context })
    }
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

pub trait FragmentService: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, crate::runtime::transport::RpcError>;

    fn send_exchange_page(
        &self,
        page: ExchangeDataPage,
    ) -> Result<(), crate::runtime::transport::RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::runtime::transport::RpcError>;
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

fn remote_source_single_qualifier(
    remote_source: &crate::planner::distributed::exchange::RemoteSourceNode,
) -> Option<TableReference> {
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

/// Worker-side fragment executor.
///
/// This owns the local execution boundary for a fragment instance. The
/// transport layer delivers requests to it, but the actual fragment execution
/// and exchange buffering live here.
pub struct LocalFragmentExecutor {
    exchange_buffers: Arc<ExchangeBufferManager>,
    storage: Arc<StorageEngine>,
    tokio_runtime: OnceLock<Runtime>,
}

impl Default for LocalFragmentExecutor {
    fn default() -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }
}

impl LocalFragmentExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage(storage: Arc<StorageEngine>) -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            storage,
            tokio_runtime: OnceLock::new(),
        }
    }

    pub fn with_exchange_buffer_manager(exchange_buffers: Arc<ExchangeBufferManager>) -> Self {
        Self {
            exchange_buffers,
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }

    fn tokio_runtime(&self) -> Result<&Runtime, crate::runtime::transport::RpcError> {
        self.tokio_runtime
            .get_or_init(|| Runtime::new().expect("tokio runtime must build"));
        self.tokio_runtime.get().ok_or_else(|| {
            crate::runtime::transport::RpcError::ExecutionFailed {
                reason: "tokio runtime was not initialized".to_owned(),
            }
        })
    }

    fn execute_streaming(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        envelope: &FragmentExecutionEnvelope,
    ) -> Result<(), crate::runtime::transport::RpcError> {
        let runtime = self.tokio_runtime()?;
        let session =
            crate::runtime::datafusion_context::session_context(&query_context).map_err(|err| {
                crate::runtime::transport::RpcError::ExecutionFailed {
                    reason: err.to_string(),
                }
            })?;
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
                .map_err(|err| crate::runtime::transport::RpcError::ExecutionFailed {
                    reason: err.to_string(),
                })?;
            let mut stream = execute_stream(physical_plan, task_ctx).map_err(|err| {
                crate::runtime::transport::RpcError::ExecutionFailed {
                    reason: err.to_string(),
                }
            })?;
            while let Some(batch) = stream.next().await {
                let batch =
                    batch.map_err(|err| crate::runtime::transport::RpcError::ExecutionFailed {
                        reason: err.to_string(),
                    })?;
                if exchange_outputs.is_empty() {
                    if let Some(sink) = &result_batch_sink {
                        sink.send_batch(batch)?;
                    }
                    continue;
                }
                for (channel, routed_batch) in route_exchange_batch(&exchange_outputs, batch)
                    .map_err(|err| crate::runtime::transport::RpcError::ExecutionFailed {
                        reason: err.to_string(),
                    })?
                {
                    let page =
                        ExchangeDataPage::from_record_batch(channel.exchange_id, routed_batch)
                            .map_err(|err| {
                                crate::runtime::transport::RpcError::ExecutionFailed {
                                    reason: err.to_string(),
                                }
                            })?;
                    if let Some(sink) = &exchange_page_sink {
                        sink.send_page(&channel, page)?;
                    } else {
                        exchange_buffers.enqueue_page(page).map_err(|err| {
                            crate::runtime::transport::RpcError::ExecutionFailed {
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
                        crate::runtime::transport::RpcError::ExecutionFailed {
                            reason: err.to_string(),
                        }
                    })?;
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    fn send_exchange_page_inner(
        &self,
        page: ExchangeDataPage,
    ) -> Result<(), crate::runtime::ExchangeRuntimeError> {
        self.exchange_buffers.enqueue_page(page)
    }

    fn drain_exchange_pages_inner(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::runtime::ExchangeRuntimeError> {
        self.exchange_buffers.drain_pages_by_id(exchange_id)
    }

    fn materialize_exchange_inputs(
        &self,
        plan: DataFusionLogicalPlan,
        exchange_inputs: &[crate::runtime::exchange::ExchangeChannelDescriptor],
    ) -> Result<DataFusionLogicalPlan, crate::runtime::transport::RpcError> {
        let exchange_inputs = exchange_inputs
            .iter()
            .map(|channel| (channel.source_fragment_id, channel.clone()))
            .collect::<HashMap<_, _>>();

        plan.transform_down(|node| match &node {
            DataFusionLogicalPlan::Extension(extension) => {
                let Some(remote_source) = extension
                    .node
                    .as_any()
                    .downcast_ref::<crate::planner::distributed::exchange::RemoteSourceNode>(
                ) else {
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
        .map_err(|err| crate::runtime::transport::RpcError::ExecutionFailed {
            reason: err.to_string(),
        })
    }
}

impl FragmentService for LocalFragmentExecutor {
    fn execute_fragment(
        &self,
        _worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, crate::runtime::transport::RpcError> {
        let FragmentExecutionEnvelope { instance, .. } = &envelope;
        let prepared = LocalFragmentPlan::prepare(
            instance.query_context.clone(),
            instance.execution_fragment.fragment.clone(),
            instance.table_catalogs.clone(),
            instance.table_scan_splits.clone(),
            Arc::clone(&self.storage),
        )
        .map_err(|err| crate::runtime::transport::RpcError::ExecutionFailed {
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

    fn send_exchange_page(
        &self,
        page: ExchangeDataPage,
    ) -> Result<(), crate::runtime::transport::RpcError> {
        self.send_exchange_page_inner(page).map_err(|err| {
            crate::runtime::transport::RpcError::ExecutionFailed {
                reason: err.to_string(),
            }
        })
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::runtime::transport::RpcError> {
        self.drain_exchange_pages_inner(exchange_id).map_err(|err| {
            crate::runtime::transport::RpcError::ExecutionFailed {
                reason: err.to_string(),
            }
        })
    }
}

impl WorkerExchangeService for LocalFragmentExecutor {
    fn send_exchange_page(
        &self,
        page: ExchangeDataPage,
    ) -> Result<(), crate::execution::exchange::WorkerExchangeError> {
        self.send_exchange_page_inner(page)
            .map_err(crate::execution::exchange::WorkerExchangeError::from)
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::execution::exchange::WorkerExchangeError> {
        self.drain_exchange_pages_inner(exchange_id)
            .map_err(crate::execution::exchange::WorkerExchangeError::from)
    }
}
