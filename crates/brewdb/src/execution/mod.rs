//! BrewDB execution contracts.

pub mod fragment;
pub mod physical_plan;

pub use fragment::{
    DataFusionFragmentExecutor, FragmentExecutionRequest, FragmentExecutionStatus,
    FragmentExecutor, FragmentExecutorError,
};
