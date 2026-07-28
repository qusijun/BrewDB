//! Scheduler policy and runnable-set coordination.

use brewdb_core::ids::{StageId, TaskId};
use brewdb_execution::boundaries::{BoundaryDescriptor, BoundarySemantics, ReleaseCondition};
use brewdb_execution::plan::{StageGraph, TaskPlan};

use crate::errors::RuntimeError;

/// Lightweight worker capacity view used by dispatch policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerSlot {
    pub worker_id: String,
    pub available_slots: u32,
}

/// Scheduling-time view of one runnable task.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunnableTask {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub preferred_workers: Vec<String>,
}

/// Current readiness facts observed by the coordinator.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SchedulingSnapshot {
    pub completed_stage_ids: Vec<StageId>,
    pub completed_task_ids: Vec<TaskId>,
    pub published_boundary_stage_ids: Vec<StageId>,
}

/// Scheduler policy surface.
pub trait SchedulingPolicy {
    fn collect_runnable(
        &self,
        graph: &StageGraph,
        snapshot: &SchedulingSnapshot,
    ) -> Result<Vec<RunnableTask>, RuntimeError>;

    fn assign_workers(
        &self,
        runnable: Vec<RunnableTask>,
        workers: &[WorkerSlot],
    ) -> Result<Vec<DispatchDecision>, RuntimeError>;
}

/// Final dispatch decision emitted by the coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchDecision {
    pub task_id: TaskId,
    pub stage_id: StageId,
    pub worker_id: String,
}

/// Phase 1 default policy: dependency-driven MPP scheduling.
#[derive(Clone, Debug, Default)]
pub struct DependencyDrivenMppPolicy;

impl SchedulingPolicy for DependencyDrivenMppPolicy {
    fn collect_runnable(
        &self,
        graph: &StageGraph,
        snapshot: &SchedulingSnapshot,
    ) -> Result<Vec<RunnableTask>, RuntimeError> {
        let mut runnable = Vec::new();

        for task in &graph.tasks {
            if snapshot.completed_task_ids.contains(&task.task_id) {
                continue;
            }

            let boundary = graph
                .stage_boundary(&task.stage_id)
                .ok_or(RuntimeError::NotFound {
                    entity: "stage_boundary",
                    id: task.stage_id.to_string(),
                })?;

            if is_task_runnable(task, boundary, snapshot) {
                runnable.push(RunnableTask {
                    task_id: task.task_id.clone(),
                    stage_id: task.stage_id.clone(),
                    preferred_workers: Vec::new(),
                });
            }
        }

        Ok(runnable)
    }

    fn assign_workers(
        &self,
        runnable: Vec<RunnableTask>,
        workers: &[WorkerSlot],
    ) -> Result<Vec<DispatchDecision>, RuntimeError> {
        if workers.is_empty() && !runnable.is_empty() {
            return Err(RuntimeError::MissingField {
                entity: "dispatch",
                field: "workers",
            });
        }

        let mut worker_cycle = workers.iter().flat_map(|worker| {
            std::iter::repeat(worker.worker_id.clone()).take(worker.available_slots as usize)
        });

        let mut decisions = Vec::new();
        for task in runnable {
            let worker_id = worker_cycle.next().ok_or(RuntimeError::StateConflict {
                entity: "dispatch",
                reason: "insufficient worker slots for runnable tasks".to_owned(),
            })?;
            decisions.push(DispatchDecision {
                task_id: task.task_id,
                stage_id: task.stage_id,
                worker_id,
            });
        }

        Ok(decisions)
    }
}

fn is_task_runnable(
    task: &TaskPlan,
    boundary: BoundaryDescriptor,
    snapshot: &SchedulingSnapshot,
) -> bool {
    if task.dependencies.is_empty() {
        return true;
    }

    match boundary.release_condition {
        ReleaseCondition::AnyUpstreamPartitionReady => task
            .dependencies
            .iter()
            .any(|dependency| is_dependency_satisfied(dependency, boundary.semantics, snapshot)),
        ReleaseCondition::AllUpstreamPartitionsReady => task
            .dependencies
            .iter()
            .all(|dependency| is_dependency_satisfied(dependency, boundary.semantics, snapshot)),
        ReleaseCondition::BoundaryArtifactsPublished => snapshot
            .published_boundary_stage_ids
            .contains(&task.dependencies[0].upstream_stage_id),
    }
}

fn is_dependency_satisfied(
    dependency: &brewdb_execution::task::TaskDependency,
    semantics: BoundarySemantics,
    snapshot: &SchedulingSnapshot,
) -> bool {
    match semantics {
        BoundarySemantics::Pipelined => dependency
            .upstream_task_id
            .as_ref()
            .map(|task_id| snapshot.completed_task_ids.contains(task_id))
            .unwrap_or_else(|| {
                snapshot
                    .completed_stage_ids
                    .contains(&dependency.upstream_stage_id)
            }),
        BoundarySemantics::Materialized | BoundarySemantics::Barriered => snapshot
            .completed_stage_ids
            .contains(&dependency.upstream_stage_id),
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId, TaskId};
    use brewdb_execution::plan::{StageGraph, StageKind, StagePlan, TaskPlan};
    use brewdb_execution::task::TaskDependency;

    use super::{
        DependencyDrivenMppPolicy, DispatchDecision, SchedulingPolicy, SchedulingSnapshot,
        WorkerSlot,
    };

    fn sample_graph() -> StageGraph {
        let source_stage_id = StageId::parse_str("550e8400-e29b-41d4-a716-446655440500").unwrap();
        let downstream_stage_id =
            StageId::parse_str("550e8400-e29b-41d4-a716-446655440501").unwrap();
        let source_task_id = TaskId::parse_str("550e8400-e29b-41d4-a716-446655440502").unwrap();
        let downstream_task_id = TaskId::parse_str("550e8400-e29b-41d4-a716-446655440503").unwrap();

        StageGraph::new(
            JobId::parse_str("550e8400-e29b-41d4-a716-446655440504").unwrap(),
            vec![
                StagePlan {
                    stage_id: source_stage_id.clone(),
                    kind: StageKind::Compute,
                    boundary: Some(BoundaryKind::Exchange),
                },
                StagePlan {
                    stage_id: downstream_stage_id.clone(),
                    kind: StageKind::Exchange,
                    boundary: Some(BoundaryKind::Exchange),
                },
            ],
        )
        .with_tasks(vec![
            TaskPlan {
                task_id: source_task_id.clone(),
                stage_id: source_stage_id.clone(),
                partition_id: 0,
                dependencies: Vec::new(),
            },
            TaskPlan {
                task_id: downstream_task_id,
                stage_id: downstream_stage_id,
                partition_id: 0,
                dependencies: vec![TaskDependency {
                    upstream_stage_id: source_stage_id,
                    upstream_task_id: Some(source_task_id),
                    partition_id: 0,
                }],
            },
        ])
    }

    #[test]
    fn policy_releases_source_tasks_first() {
        let policy = DependencyDrivenMppPolicy;
        let runnable = policy
            .collect_runnable(&sample_graph(), &SchedulingSnapshot::default())
            .unwrap();

        assert_eq!(runnable.len(), 1);
    }

    #[test]
    fn policy_releases_downstream_task_after_dependency_completion() {
        let policy = DependencyDrivenMppPolicy;
        let downstream_task_id = TaskId::parse_str("550e8400-e29b-41d4-a716-446655440503").unwrap();
        let snapshot = SchedulingSnapshot {
            completed_stage_ids: Vec::new(),
            completed_task_ids: vec![
                TaskId::parse_str("550e8400-e29b-41d4-a716-446655440502").unwrap(),
            ],
            published_boundary_stage_ids: Vec::new(),
        };

        let runnable = policy.collect_runnable(&sample_graph(), &snapshot).unwrap();

        assert_eq!(runnable.len(), 1);
        assert_eq!(runnable[0].task_id, downstream_task_id);
    }

    #[test]
    fn policy_assigns_workers_by_available_slots() {
        let policy = DependencyDrivenMppPolicy;
        let decisions = policy
            .assign_workers(
                vec![
                    super::RunnableTask {
                        task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440510").unwrap(),
                        stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440511")
                            .unwrap(),
                        preferred_workers: Vec::new(),
                    },
                    super::RunnableTask {
                        task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440512").unwrap(),
                        stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440513")
                            .unwrap(),
                        preferred_workers: Vec::new(),
                    },
                ],
                &[WorkerSlot {
                    worker_id: "worker-a".to_owned(),
                    available_slots: 2,
                }],
            )
            .unwrap();

        assert_eq!(
            decisions,
            vec![
                DispatchDecision {
                    task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440510").unwrap(),
                    stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440511").unwrap(),
                    worker_id: "worker-a".to_owned(),
                },
                DispatchDecision {
                    task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440512").unwrap(),
                    stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440513").unwrap(),
                    worker_id: "worker-a".to_owned(),
                },
            ]
        );
    }
}
