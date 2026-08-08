//! Fragment scheduling contracts.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use brewdb_common::runtime::QueryContext;
use brewdb_planner::plan::{DistributedPlan, PlanFragment, PlanFragmentKind};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerInfo {
    pub worker_id: Uuid,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledFragment {
    pub query_context: QueryContext,
    pub fragment: PlanFragment,
    pub worker_id: Uuid,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FragmentSchedule {
    pub query_context: QueryContext,
    pub fragments: Vec<ScheduledFragment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentSchedulerError {
    EmptyPlan,
    NoAvailableWorker,
}

impl fmt::Display for FragmentSchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPlan => write!(f, "distributed plan has no fragments"),
            Self::NoAvailableWorker => write!(f, "no available workers"),
        }
    }
}

impl Error for FragmentSchedulerError {}

pub trait ResourceManager: Send + Sync {
    fn workers(&self) -> Vec<WorkerInfo>;
}

pub trait WorkerSelector: Send + Sync {
    fn select_worker(
        &self,
        workers: &[WorkerInfo],
        fragment: &PlanFragment,
    ) -> Result<WorkerInfo, FragmentSchedulerError>;
}

#[derive(Clone, Debug)]
pub struct StaticResourceManager {
    workers: Vec<WorkerInfo>,
}

impl StaticResourceManager {
    pub fn new(workers: Vec<WorkerInfo>) -> Self {
        Self { workers }
    }
}

impl ResourceManager for StaticResourceManager {
    fn workers(&self) -> Vec<WorkerInfo> {
        self.workers.clone()
    }
}

#[derive(Clone, Debug, Default)]
pub struct FirstWorkerSelector;

impl WorkerSelector for FirstWorkerSelector {
    fn select_worker(
        &self,
        workers: &[WorkerInfo],
        _fragment: &PlanFragment,
    ) -> Result<WorkerInfo, FragmentSchedulerError> {
        workers
            .first()
            .cloned()
            .ok_or(FragmentSchedulerError::NoAvailableWorker)
    }
}

pub trait FragmentScheduler {
    fn schedule(
        &self,
        plan: DistributedPlan,
        resource_manager: &dyn ResourceManager,
    ) -> Result<FragmentSchedule, FragmentSchedulerError>;
}

#[derive(Clone)]
pub struct AllAtOnceFragmentScheduler {
    pub worker_selector: Arc<dyn WorkerSelector>,
}

impl Default for AllAtOnceFragmentScheduler {
    fn default() -> Self {
        Self {
            worker_selector: std::sync::Arc::new(FirstWorkerSelector),
        }
    }
}

impl FragmentScheduler for AllAtOnceFragmentScheduler {
    fn schedule(
        &self,
        plan: DistributedPlan,
        resource_manager: &dyn ResourceManager,
    ) -> Result<FragmentSchedule, FragmentSchedulerError> {
        if plan.fragments.is_empty() {
            return Err(FragmentSchedulerError::EmptyPlan);
        }
        let workers = resource_manager.workers();
        let mut fragments = plan.fragments;
        fragments.sort_by_key(|fragment| match fragment.kind {
            PlanFragmentKind::Source => 0u8,
            PlanFragmentKind::Intermediate => 1,
            PlanFragmentKind::Root => 2,
        });
        let fragments = fragments
            .into_iter()
            .map(|fragment| {
                let worker = self.worker_selector.select_worker(&workers, &fragment)?;
                Ok(ScheduledFragment {
                    query_context: plan.query_context.clone(),
                    fragment,
                    worker_id: worker.worker_id,
                    endpoint: worker.endpoint,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FragmentSchedule {
            query_context: plan.query_context.clone(),
            fragments,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use brewdb_planner::PlanStageId;
    use brewdb_planner::plan::{DistributedPlan, PlanFragment, PlanFragmentId, PlanFragmentKind};

    use super::{
        AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, FragmentSchedulerError,
        StaticResourceManager, WorkerInfo,
    };
    use brewdb_common::runtime::QueryContext;

    #[test]
    fn scheduler_rejects_empty_plan() {
        let scheduler = AllAtOnceFragmentScheduler::default();
        let err = scheduler
            .schedule(
                DistributedPlan {
                    query_context: QueryContext {
                        query_id: uuid::Uuid::new_v4(),
                    },
                    fragments: vec![],
                    exchanges: vec![],
                },
                &StaticResourceManager::new(vec![WorkerInfo {
                    worker_id: uuid::Uuid::new_v4(),
                    endpoint: "rpc://worker-1".to_owned(),
                }]),
            )
            .unwrap_err();
        assert_eq!(err, FragmentSchedulerError::EmptyPlan);
    }

    #[test]
    fn scheduler_assigns_every_fragment_to_one_worker() {
        let worker_id = uuid::Uuid::new_v4();
        let scheduler = AllAtOnceFragmentScheduler {
            worker_selector: Arc::new(FirstWorkerSelector),
        };
        let plan = DistributedPlan {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragments: vec![PlanFragment {
                fragment_id: PlanFragmentId {
                    stage_id: PlanStageId(0),
                    fragment_ordinal: 0,
                },
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: None,
            }],
            exchanges: vec![],
        };

        let scheduled = scheduler
            .schedule(
                plan,
                &StaticResourceManager::new(vec![WorkerInfo {
                    worker_id,
                    endpoint: "rpc://worker-1".to_owned(),
                }]),
            )
            .unwrap();
        assert_eq!(scheduled.fragments.len(), 1);
        assert_eq!(scheduled.fragments[0].worker_id, worker_id);
        assert_eq!(scheduled.fragments[0].endpoint, "rpc://worker-1");
    }
}
