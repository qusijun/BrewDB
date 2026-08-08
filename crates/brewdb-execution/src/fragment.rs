//! DataFusion-backed fragment execution contracts.

use std::error::Error;
use std::fmt;
use std::sync::OnceLock;

use brewdb_common::runtime::QueryContext;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;
use tokio::runtime::Runtime;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FragmentExecutionRequest {
    pub query_context: QueryContext,
    pub logical_plan: DataFusionLogicalPlan,
}

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
        request: FragmentExecutionRequest,
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
        request: FragmentExecutionRequest,
    ) -> Result<FragmentExecutionStatus, FragmentExecutorError> {
        let runtime = self.tokio_runtime()?;
        let session = SessionContext::new();
        runtime.block_on(async move {
            let df = session
                .execute_logical_plan(request.logical_plan)
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
        Ok(FragmentExecutionStatus {
            query_context: request.query_context,
        })
    }
}
