//! BrewDB execution contracts.

pub mod context;
pub mod exchange;
pub mod executor;
pub mod fragment;
pub mod physical_plan;

pub use exchange::{WorkerExchangeError, WorkerExchangeService};
pub use executor::{
    DataFusionFragmentExecutor, FragmentExecutionEnvelope, FragmentExecutionStatus,
    FragmentExecutor, FragmentExecutorError, FragmentService, LocalFragmentExecutor,
};
pub use fragment::{ExecutionFragment, FragmentInstance};

#[cfg(test)]
mod tests {
    use crate::common::config::ConfigSet;
    use crate::common::context::QueryContext;
    use uuid::Uuid;

    #[test]
    fn execution_context_builds_datafusion_session_from_query_context() {
        let query_context = QueryContext::for_test(Uuid::new_v4())
            .with_settings(ConfigSet::new().with_entry("datafusion.execution.batch_size", 256_u64));

        let session = crate::execution::context::session_context(&query_context).unwrap();

        assert_eq!(
            session
                .copied_config()
                .options()
                .as_ref()
                .execution
                .batch_size,
            256
        );
    }
}
