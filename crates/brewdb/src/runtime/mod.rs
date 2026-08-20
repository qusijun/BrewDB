//! BrewDB runtime contracts.

pub mod coordinator;
pub mod datafusion_context;
pub mod driver;
pub mod exchange;
pub mod exchange_service;
pub mod execution_graph;
pub mod function;
pub mod scheduler;
pub mod storage;
pub mod transport;

pub use crate::execution::{FragmentExecutionEnvelope, FragmentService};
pub use crate::planner::LocalFragmentPlan;
pub use coordinator::QueryCoordinator;
pub use driver::{SqlDriver, SqlDriverError};
pub use exchange::{
    build_exchange_channels, ExchangeBufferManager, ExchangeChannelDescriptor,
    ExchangeDataEncoding, ExchangeDataPage, ExchangeId, ExchangeRuntimeError,
};
pub use exchange_service::{ExchangePageSink, ResultBatchSink, TransportExchangePageSink};
pub use execution_graph::{
    ExecutionFragment, ExecutionGraph, ExecutionRuntimeError, FragmentInstance,
    QueryExecutionHandle, QueryOutput,
};
pub use scheduler::{
    AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, FragmentSchedulerError,
    ResourceManager, StaticResourceManager, WorkerInfo, WorkerSelector,
};
pub use storage::build_storage_engine;
pub use transport::{
    FragmentTransport, LocalFragmentTransport, RpcClient, RpcError, TransportRegistry,
    TransportRpcClient,
};
