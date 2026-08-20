//! BrewDB execution contracts.

pub mod exchange;
pub mod executor;
pub mod physical_plan;

pub use exchange::{WorkerExchangeError, WorkerExchangeService};
pub use executor::{
    DataFusionFragmentExecutor, FragmentExecutionEnvelope, FragmentExecutionStatus,
    FragmentExecutor, FragmentExecutorError, FragmentService, LocalFragmentExecutor,
};
