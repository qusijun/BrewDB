//! Fragment scheduling contracts.

use std::sync::Arc;

use crate::planner::distributed::{FragmentInstance, PlanFragment, PlanFragmentKind};
use uuid::Uuid;

use crate::runtime::errors::FragmentSchedulerError;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::storage::{TableScanSplitGroup, TableSourceId};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerInfo {
    pub worker_id: Uuid,
    pub endpoint: String,
}

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
        table_scan_splits: TableScanSplitGroup,
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
        table_scan_splits: TableScanSplitGroup,
        resource_manager: &dyn ResourceManager,
    ) -> Result<ExecutionGraph, FragmentSchedulerError> {
        if execution_graph.fragments.is_empty() {
            return Err(FragmentSchedulerError::EmptyPlan);
        }
        let workers = resource_manager.workers();
        execution_graph
            .fragments
            .sort_by_key(|fragment| match fragment.kind {
                PlanFragmentKind::Source => 0u8,
                PlanFragmentKind::Intermediate => 1,
                PlanFragmentKind::Root => 2,
            });
        execution_graph.instances.clear();
        let mut instances = Vec::new();
        for fragment in &execution_graph.fragments {
            let fragment_id = fragment.fragment_id;
            let assigned_splits = table_scan_splits
                .splits_for_table_source(TableSourceId(fragment_id.0))
                .unwrap_or_default();
            let worker = self.worker_selector.select_worker(&workers, fragment)?;
            let table_scan_splits = if fragment.kind == PlanFragmentKind::Source {
                assigned_splits.to_vec()
            } else {
                Vec::new()
            };
            instances.push(FragmentInstance::scheduled(
                instances.len() as u32,
                fragment.clone(),
                worker.worker_id,
                worker.endpoint,
                table_scan_splits,
            ));
        }
        execution_graph.instances = instances;
        Ok(execution_graph)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::planner::distributed::{PlanFragment, PlanFragmentId, PlanFragmentKind};

    use crate::common::context::QueryContext;
    use crate::storage::{TableScanSplit, TableScanSplitGroup, TableSourceId};

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
                    query_context: QueryContext::for_test(uuid::Uuid::new_v4()),
                    fragments: vec![],
                    instances: vec![],
                },
                TableScanSplitGroup::default(),
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
            QueryContext::for_test(uuid::Uuid::new_v4()),
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
                TableScanSplitGroup::default(),
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

    #[test]
    fn scheduler_splits_source_table_scan_splits_into_pipeline_instances() {
        let fragment_id = PlanFragmentId(7);
        let scheduler = AllAtOnceFragmentScheduler {
            worker_selector: Arc::new(FirstWorkerSelector),
        };
        let execution_graph = ExecutionGraph::from_plan_fragments(
            QueryContext::for_test(uuid::Uuid::new_v4()),
            vec![PlanFragment {
                fragment_id,
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: None,
            }],
        );

        let scheduled = scheduler
            .schedule(
                execution_graph,
                TableScanSplitGroup::from_table_source(
                    TableSourceId(fragment_id.0),
                    vec![
                        TableScanSplit::new("orders", 0),
                        TableScanSplit::new("orders", 1),
                        TableScanSplit::new("orders", 2),
                    ],
                ),
                &StaticResourceManager::new(vec![WorkerInfo {
                    worker_id: uuid::Uuid::new_v4(),
                    endpoint: "rpc://worker-1".to_owned(),
                }]),
            )
            .unwrap();

        assert_eq!(scheduled.instances.len(), 1);
        assert_eq!(scheduled.instances[0].table_scan_splits.len(), 3);
        assert_eq!(
            scheduled.instances[0]
                .table_scan_splits
                .iter()
                .map(|split| split.ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }
}
