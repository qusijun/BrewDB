//! BrewDB runtime contracts.

pub mod coordinator;
pub mod driver;
pub mod errors;
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
pub use driver::SqlDriver;
pub use errors::{
    ExchangeRuntimeError, ExecutionRuntimeError, FragmentSchedulerError, SqlDriverError,
};
pub use exchange::{
    build_exchange_channels, ExchangeBufferManager, ExchangeChannelDescriptor,
    ExchangeDataEncoding, ExchangeDataPage, ExchangeId,
};
pub use exchange_service::{ExchangePageSink, ResultBatchSink, TransportExchangePageSink};
pub use execution_graph::{
    ExecutionFragment, ExecutionGraph, FragmentInstance, QueryExecutionHandle, QueryOutput,
};
pub use scheduler::{
    AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, ResourceManager,
    StaticResourceManager, WorkerInfo, WorkerSelector,
};
pub use storage::build_storage_engine;
pub use transport::{
    FragmentTransport, LocalFragmentTransport, RpcClient, RpcError, TransportRegistry,
    TransportRpcClient,
};
