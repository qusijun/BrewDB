//! Worker-side fragment executor contracts.

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

use crate::common::context::QueryContext;
use crate::common::diagnostics::{DiagnosticContext, DiagnosticError, ErrorCode};
use crate::common::profile::FragmentProfile;
use crate::execution::exchange::WorkerExchangeService;
use crate::planner::LocalFragmentPlan;
use crate::runtime::RpcError;
use crate::runtime::exchange::{
    ExchangeBufferManager, ExchangeDataPage, ExchangeId, route_exchange_batch,
};
use crate::runtime::exchange_service::{ExchangePageSink, ResultBatchSink};
use crate::runtime::fragment::FragmentInstance;
use crate::storage::{StorageEngine, TableScanSplitGroup, TableSourceId};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::provider_as_source;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion_common::DataFusionError;
use datafusion_common::TableReference;
use datafusion_common::error::Result as DataFusionResult;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use datafusion_expr::{LogicalPlanBuilder, TableType};
use futures::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Runtime;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentExecutionStatus {
    pub query_context: QueryContext,
    pub profile: Option<FragmentProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentExecutorError {
    InvalidPlan { reason: String },
    RuntimeInitFailed { reason: String },
}

const FRAGMENT_EXECUTOR_INVALID_PLAN: ErrorCode =
    ErrorCode::new("BREWDB_EXECUTION_FRAGMENT_EXECUTOR_INVALID_PLAN");
const FRAGMENT_EXECUTOR_RUNTIME_INIT_FAILED: ErrorCode =
    ErrorCode::new("BREWDB_EXECUTION_FRAGMENT_EXECUTOR_RUNTIME_INIT_FAILED");

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

impl DiagnosticError for FragmentExecutorError {
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::InvalidPlan { .. } => FRAGMENT_EXECUTOR_INVALID_PLAN,
            Self::RuntimeInitFailed { .. } => FRAGMENT_EXECUTOR_RUNTIME_INIT_FAILED,
        }
    }

    fn log_target(&self) -> &'static str {
        "brewdb.execution"
    }

    fn diagnostic_context(&self, event_name: &'static str) -> DiagnosticContext {
        DiagnosticContext::new(self.log_target(), event_name)
            .with_error_code(self.error_code())
            .with_error_variant(self.variant_name())
    }
}

impl FragmentExecutorError {
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::InvalidPlan { .. } => "InvalidPlan",
            Self::RuntimeInitFailed { .. } => "RuntimeInitFailed",
        }
    }
}

#[derive(Clone)]
pub struct FragmentExecutionEnvelope {
    pub instance: FragmentInstance,
    /// True when this envelope executes the single-fragment standalone path.
    ///
    /// Standalone keeps all table scans inside one root fragment and therefore
    /// prepares scan providers from the query-level split group. Distributed
    /// execution prepares providers from the split list assigned to the
    /// fragment instance.
    pub standalone: bool,
    /// Scan split assignments passed to the worker.
    ///
    /// Workers use this only when `standalone` is true. Distributed workers use
    /// `FragmentInstance::table_scan_splits` instead.
    pub table_scan_splits: TableScanSplitGroup,
    pub exchange_page_sink: Option<Arc<dyn ExchangePageSink>>,
    pub result_batch_sink: Option<Arc<dyn ResultBatchSink>>,
}

impl FragmentExecutionEnvelope {
    pub fn new(instance: FragmentInstance) -> Self {
        Self {
            instance,
            standalone: false,
            table_scan_splits: TableScanSplitGroup::default(),
            exchange_page_sink: None,
            result_batch_sink: None,
        }
    }

    pub fn with_standalone(mut self, standalone: bool) -> Self {
        self.standalone = standalone;
        self
    }

    pub fn with_table_scan_splits(mut self, table_scan_splits: TableScanSplitGroup) -> Self {
        self.table_scan_splits = table_scan_splits;
        self
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
    ) -> Result<FragmentExecutionStatus, crate::runtime::RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), crate::runtime::RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::runtime::RpcError>;
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
                                Err(datafusion_common::DataFusionError::External(Box::new(err))),
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

struct FragmentPhysicalPlan {
    physical_plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

struct FragmentOutputSinks {
    exchange_outputs: Vec<crate::runtime::exchange::ExchangeChannelDescriptor>,
    exchange_page_sink: Option<Arc<dyn ExchangePageSink>>,
    result_batch_sink: Option<Arc<dyn ResultBatchSink>>,
    exchange_buffers: Arc<ExchangeBufferManager>,
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

    fn tokio_runtime(&self) -> Result<&Runtime, crate::runtime::RpcError> {
        self.tokio_runtime
            .get_or_init(|| Runtime::new().expect("tokio runtime must build"));
        self.tokio_runtime
            .get()
            .ok_or_else(|| crate::runtime::RpcError::ExecutionFailed {
                reason: "tokio runtime was not initialized".to_owned(),
                cause: None,
            })
    }

    fn create_physical_plan(
        &self,
        query_context: &QueryContext,
        logical_plan: DataFusionLogicalPlan,
    ) -> Result<FragmentPhysicalPlan, crate::runtime::RpcError> {
        let runtime = self.tokio_runtime()?;
        let session =
            crate::execution::context::session_context(&query_context).map_err(|err| {
                let reason = err.to_string();
                crate::runtime::RpcError::ExecutionFailed {
                    reason,
                    cause: Some(err),
                }
            })?;
        let physical_plan = runtime.block_on(async move {
            let state = session.state();
            let task_ctx = session.task_ctx();
            let physical_plan = state
                .create_physical_plan(&logical_plan)
                .await
                .map_err(map_datafusion_rpc_error)?;
            Ok::<_, crate::runtime::RpcError>(FragmentPhysicalPlan {
                physical_plan,
                task_ctx,
            })
        })?;
        Ok(physical_plan)
    }

    fn execute_physical_plan_streaming(
        &self,
        physical_plan: FragmentPhysicalPlan,
        output: FragmentOutputSinks,
    ) -> Result<Arc<dyn ExecutionPlan>, crate::runtime::RpcError> {
        let plan_for_profile = Arc::clone(&physical_plan.physical_plan);
        let runtime = self.tokio_runtime()?;
        runtime.block_on(async move {
            let mut stream = execute_stream(physical_plan.physical_plan, physical_plan.task_ctx)
                .map_err(map_datafusion_rpc_error)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(map_datafusion_rpc_error)?;
                if output.exchange_outputs.is_empty() {
                    if let Some(sink) = &output.result_batch_sink {
                        sink.send_batch(batch)?;
                    }
                    continue;
                }
                for (channel, routed_batch) in route_exchange_batch(&output.exchange_outputs, batch)
                    .map_err(|err| crate::runtime::RpcError::ExecutionFailed {
                        reason: err.to_string(),
                        cause: None,
                    })?
                {
                    let page =
                        ExchangeDataPage::from_record_batch(channel.exchange_id, routed_batch)
                            .map_err(|err| crate::runtime::RpcError::ExecutionFailed {
                                reason: err.to_string(),
                                cause: None,
                            })?;
                    if let Some(sink) = &output.exchange_page_sink {
                        sink.send_page(&channel, page)?;
                    } else {
                        output.exchange_buffers.enqueue_page(page).map_err(|err| {
                            crate::runtime::RpcError::ExecutionFailed {
                                reason: err.to_string(),
                                cause: None,
                            }
                        })?;
                    }
                }
            }
            for channel in &output.exchange_outputs {
                let page = ExchangeDataPage::end_of_stream(channel.exchange_id);
                if let Some(sink) = &output.exchange_page_sink {
                    sink.send_page(channel, page)?;
                } else {
                    output.exchange_buffers.enqueue_page(page).map_err(|err| {
                        crate::runtime::RpcError::ExecutionFailed {
                            reason: err.to_string(),
                            cause: None,
                        }
                    })?;
                }
            }
            Ok::<_, RpcError>(())
        })?;
        Ok(plan_for_profile)
    }

    fn execute_streaming(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        envelope: &FragmentExecutionEnvelope,
    ) -> Result<FragmentProfile, crate::runtime::RpcError> {
        let start = std::time::Instant::now();
        let physical_plan = self.create_physical_plan(&query_context, logical_plan)?;
        let output = FragmentOutputSinks {
            exchange_outputs: envelope.instance.exchange_outputs.clone(),
            exchange_page_sink: envelope.exchange_page_sink.clone(),
            result_batch_sink: envelope.result_batch_sink.clone(),
            exchange_buffers: Arc::clone(&self.exchange_buffers),
        };
        let physical_plan = self.execute_physical_plan_streaming(physical_plan, output)?;
        Ok(FragmentProfile {
            fragment_id: format!("{:?}", envelope.instance.fragment_id()),
            worker_id: Some(envelope.instance.worker_id.to_string()),
            kind: format!("{:?}", envelope.instance.fragment().kind),
            elapsed_ms: start.elapsed().as_millis() as u64,
            metrics: vec![],
            operators: vec![
                crate::runtime::profile::operator_profile_from_execution_plan(
                    physical_plan.as_ref(),
                ),
            ],
        })
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
    ) -> Result<DataFusionLogicalPlan, crate::runtime::RpcError> {
        let exchange_inputs = exchange_inputs
            .iter()
            .fold(HashMap::new(), |mut inputs, channel| {
                inputs
                    .entry(channel.source_fragment_id)
                    .or_insert_with(Vec::new)
                    .push(channel.clone());
                inputs
            });

        plan.transform_down(|node| match &node {
            DataFusionLogicalPlan::Extension(extension) => {
                let Some(remote_source) = extension
                    .node
                    .as_any()
                    .downcast_ref::<crate::planner::distributed::exchange::RemoteSourceNode>(
                ) else {
                    return Ok(Transformed::no(node));
                };
                let mut exchange_ids = Vec::new();
                for source_fragment_id in &remote_source.source_fragment_ids {
                    let channels = exchange_inputs.get(source_fragment_id).ok_or_else(|| {
                        datafusion_common::DataFusionError::Plan(format!(
                            "exchange input for fragment {:?} is missing",
                            source_fragment_id
                        ))
                    })?;
                    exchange_ids.extend(channels.iter().map(|channel| channel.exchange_id));
                }
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
                )?;
                if let Some(qualifier) = remote_source_single_qualifier(remote_source) {
                    builder = builder.alias(qualifier)?;
                }
                let rewritten = builder.build()?;
                Ok(Transformed::yes(rewritten))
            }
            _ => Ok(Transformed::no(node)),
        })
        .map(|result| result.data)
        .map_err(map_datafusion_rpc_error)
    }
}

fn map_datafusion_rpc_error(error: DataFusionError) -> RpcError {
    let reason = error.to_string();
    RpcError::ExecutionFailed {
        reason,
        cause: Some(error),
    }
}

impl FragmentService for LocalFragmentExecutor {
    fn execute_fragment(
        &self,
        _worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, crate::runtime::RpcError> {
        let FragmentExecutionEnvelope {
            instance,
            standalone,
            table_scan_splits,
            ..
        } = &envelope;
        let local_scan_assignment = if *standalone {
            Some(table_scan_splits.clone()).filter(|splits| !splits.is_empty())
        } else if instance.table_scan_splits.is_empty() {
            None
        } else {
            Some(TableScanSplitGroup::from_table_source(
                TableSourceId(instance.fragment_id().0),
                instance.table_scan_splits.clone(),
            ))
        };
        let prepared = LocalFragmentPlan::prepare(
            instance.query_context.clone(),
            instance.execution_fragment.fragment.clone(),
            instance.table_catalogs.clone(),
            local_scan_assignment,
            Arc::clone(&self.storage),
        )
        .map_err(|err| {
            let reason = err.to_string();
            crate::runtime::RpcError::ExecutionFailed {
                reason,
                cause: err.into_datafusion_cause(),
            }
        })?;
        let logical_plan = self.materialize_exchange_inputs(
            prepared.logical_plan.clone(),
            instance.exchange_inputs.as_slice(),
        )?;
        let profile =
            self.execute_streaming(prepared.query_context.clone(), logical_plan, &envelope)?;
        Ok(FragmentExecutionStatus {
            query_context: prepared.query_context,
            profile: Some(profile),
        })
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), crate::runtime::RpcError> {
        self.send_exchange_page_inner(page).map_err(|err| {
            crate::runtime::RpcError::ExecutionFailed {
                reason: err.to_string(),
                cause: None,
            }
        })
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, crate::runtime::RpcError> {
        self.drain_exchange_pages_inner(exchange_id).map_err(|err| {
            crate::runtime::RpcError::ExecutionFailed {
                reason: err.to_string(),
                cause: None,
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

#[cfg(test)]
mod tests {
    use crate::common::diagnostics::DiagnosticError;

    use super::FragmentExecutorError;

    #[test]
    fn fragment_executor_error_uses_execution_diagnostic_code() {
        let error = FragmentExecutorError::InvalidPlan {
            reason: "missing local plan".to_owned(),
        };

        let context = error.diagnostic_context("fragment.execute");

        assert_eq!(context.target, "brewdb.execution");
        assert_eq!(
            context.error_code_str(),
            Some("BREWDB_EXECUTION_FRAGMENT_EXECUTOR_INVALID_PLAN")
        );
        assert_eq!(context.error_variant, Some("InvalidPlan"));
    }
}
