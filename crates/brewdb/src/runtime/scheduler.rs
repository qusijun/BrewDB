//! Fragment scheduling contracts.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::planner::distributed::plan::{FragmentScanSplits, PlanFragment, PlanFragmentKind};
use uuid::Uuid;

use crate::runtime::execution_graph::{ExecutionGraph, FragmentInstance};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerInfo {
    pub worker_id: Uuid,
    pub endpoint: String,
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
        execution_graph: ExecutionGraph,
        fragment_scan_splits: Vec<FragmentScanSplits>,
        resource_manager: &dyn ResourceManager,
    ) -> Result<ExecutionGraph, FragmentSchedulerError>;
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
        mut execution_graph: ExecutionGraph,
        fragment_scan_splits: Vec<FragmentScanSplits>,
        resource_manager: &dyn ResourceManager,
    ) -> Result<ExecutionGraph, FragmentSchedulerError> {
        if execution_graph.fragments.is_empty() {
            return Err(FragmentSchedulerError::EmptyPlan);
        }
        let workers = resource_manager.workers();
        let split_assignments = fragment_scan_splits
            .into_iter()
            .map(|splits| (splits.fragment_id, splits.table_scan_splits))
            .collect::<std::collections::HashMap<_, _>>();
        execution_graph.fragments.sort_by_key(|execution_fragment| {
            match execution_fragment.fragment.kind {
                PlanFragmentKind::Source => 0u8,
                PlanFragmentKind::Intermediate => 1,
                PlanFragmentKind::Root => 2,
            }
        });
        execution_graph.instances.clear();
        let instances = execution_graph
            .fragments
            .iter()
            .map(|execution_fragment| {
                let worker = self
                    .worker_selector
                    .select_worker(&workers, &execution_fragment.fragment)?;
                let fragment_id = execution_fragment.fragment_id();
                Ok(FragmentInstance::scheduled(
                    Uuid::new_v4(),
                    execution_fragment.clone(),
                    worker.worker_id,
                    worker.endpoint,
                    split_assignments
                        .get(&fragment_id)
                        .cloned()
                        .unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        execution_graph.instances = instances;
        Ok(execution_graph)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::planner::distributed::plan::{PlanFragment, PlanFragmentId, PlanFragmentKind};

    use crate::common::runtime::QueryContext;

    use crate::runtime::execution_graph::ExecutionGraph;

    use super::{
        AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, FragmentSchedulerError,
        StaticResourceManager, WorkerInfo,
    };

    #[test]
    fn scheduler_rejects_empty_plan() {
        let scheduler = AllAtOnceFragmentScheduler::default();
        let err = scheduler
            .schedule(
                ExecutionGraph {
                    query_context: QueryContext {
                        query_id: uuid::Uuid::new_v4(),
                    },
                    fragments: vec![],
                    instances: vec![],
                },
                vec![],
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
        let execution_graph = ExecutionGraph::from_plan_fragments(
            QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            vec![PlanFragment {
                fragment_id: PlanFragmentId(0),
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: None,
            }],
        );

        let scheduled = scheduler
            .schedule(
                execution_graph,
                vec![],
                &StaticResourceManager::new(vec![WorkerInfo {
                    worker_id,
                    endpoint: "rpc://worker-1".to_owned(),
                }]),
            )
            .unwrap();
        assert_eq!(scheduled.fragments.len(), 1);
        assert_eq!(scheduled.instances.len(), 1);
        assert_eq!(scheduled.instances[0].fragment_id(), PlanFragmentId(0));
        assert_eq!(scheduled.instances[0].worker_id, worker_id);
        assert_eq!(scheduled.instances[0].endpoint, "rpc://worker-1");
    }
}
