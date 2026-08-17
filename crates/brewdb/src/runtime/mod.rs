//! BrewDB runtime contracts.

pub mod coordinator;
pub mod driver;
pub mod exchange;
pub mod execution_graph;
pub mod function;
pub mod rpc;
pub mod scheduler;
pub mod storage;

pub use crate::planner::LocalFragmentPlan;
pub use coordinator::QueryCoordinator;
pub use driver::{SqlDriver, SqlDriverError};
pub use exchange::{
    build_exchange_channels, ExchangeBufferManager, ExchangeChannelDescriptor,
    ExchangeDataEncoding, ExchangeDataPage, ExchangeId, ExchangeRuntimeError,
};
pub use execution_graph::{
    ExecutionFragment, ExecutionGraph, ExecutionRuntimeError, FragmentInstance,
    QueryExecutionHandle, QueryExecutionRequest, QueryOutput,
};
pub use rpc::{
    FragmentExecutionEnvelope, FragmentService, FragmentTransport, LocalFragmentExecutor,
    LocalFragmentTransport, ResultBatchSink, RpcClient, RpcError, TransportRegistry,
    TransportRpcClient,
};
pub use scheduler::{
    AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, FragmentSchedulerError,
    ResourceManager, StaticResourceManager, WorkerInfo, WorkerSelector,
};
pub use storage::build_storage_engine;
