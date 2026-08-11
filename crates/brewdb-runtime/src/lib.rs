//! BrewDB runtime contracts.

pub mod driver;
pub mod exchange;
pub mod execution;
pub mod rpc;
pub mod scheduler;
pub mod storage;

pub use brewdb_planner::LocalFragmentPlan;
pub use driver::{SqlDriver, SqlDriverError};
pub use exchange::{
    ExchangeBufferManager, ExchangeChannelDescriptor, ExchangeDataEncoding, ExchangeDataPage,
    ExchangeId, ExchangeRuntimeError, build_exchange_channels,
};
pub use execution::{
    DistributedExecutionRuntime, ExecutionRuntime, ExecutionRuntimeError, FragmentDispatch,
    QueryExecutionHandle, QueryExecutionRequest, QueryOutput,
};
pub use rpc::{
    FragmentService, FragmentTask, FragmentTransport, LocalFragmentService, LocalFragmentTransport,
    ResultBatchSink, RpcClient, RpcError, TransportRegistry, TransportRpcClient,
};
pub use scheduler::{
    AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentSchedule, FragmentScheduler,
    FragmentSchedulerError, ResourceManager, ScheduledFragment, StaticResourceManager, WorkerInfo,
    WorkerSelector,
};
pub use storage::build_storage_engine;
