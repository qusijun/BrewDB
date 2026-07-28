//! Coordinator admission entry shell.

use brewdb_core::ids::{JobId, StageId};

use crate::dispatcher::{DispatchWave, RegisterExecutionGraph};
use crate::errors::RuntimeError;
use crate::jobs::SubmitJob;
use crate::planning::{BuildPlan, OrchestrationPlan};
use crate::runtime_meta::JobRecord;
use crate::scheduler::{SchedulingSnapshot, WorkerSlot};

/// Top-level coordinator admission command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmitRequest {
    pub build_plan: BuildPlan,
}

/// Bootstrap state emitted once a request is admitted into runtime ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionBootstrap {
    pub job_record: JobRecord,
    pub orchestration_plan: OrchestrationPlan,
    pub register_execution_graph: Option<RegisterExecutionGraph>,
}

impl AdmissionBootstrap {
    pub fn stage_ids(&self) -> Vec<StageId> {
        self.register_execution_graph
            .as_ref()
            .map(|command| command.stage_ids())
            .unwrap_or_default()
    }
}

/// Command to open the first or next dispatch wave after admission/bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartDispatch {
    pub job_id: JobId,
    pub snapshot: SchedulingSnapshot,
    pub workers: Vec<WorkerSlot>,
}

impl StartDispatch {
    pub fn into_dispatch_wave(self) -> DispatchWave {
        DispatchWave {
            job_id: self.job_id,
            snapshot: self.snapshot,
            workers: self.workers,
        }
    }
}

/// Top-level coordinator admission boundary.
pub trait AdmissionService {
    fn admit_request(&self, command: AdmitRequest) -> Result<AdmissionBootstrap, RuntimeError>;

    fn start_dispatch(&self, command: StartDispatch) -> Result<DispatchWave, RuntimeError>;
}

/// Phase 1 direct admission service that keeps the first query loop explicit.
#[derive(Clone, Debug, Default)]
pub struct DirectAdmissionService;

impl AdmissionService for DirectAdmissionService {
    fn admit_request(&self, command: AdmitRequest) -> Result<AdmissionBootstrap, RuntimeError> {
        command.bootstrap()
    }

    fn start_dispatch(&self, command: StartDispatch) -> Result<DispatchWave, RuntimeError> {
        Ok(command.into_dispatch_wave())
    }
}

impl AdmitRequest {
    pub fn into_submit_job(&self) -> SubmitJob {
        self.build_plan.into_submit_job()
    }

    pub fn bootstrap(self) -> Result<AdmissionBootstrap, RuntimeError> {
        let job_record = self.into_submit_job().into_record();
        let orchestration_plan = self.build_plan.build()?;
        let register_execution_graph =
            orchestration_plan
                .stage_graph
                .clone()
                .map(|stage_graph| RegisterExecutionGraph {
                    job_id: orchestration_plan.job_id.clone(),
                    stage_graph,
                });

        Ok(AdmissionBootstrap {
            job_record,
            orchestration_plan,
            register_execution_graph,
        })
    }
}

#[cfg(test)]
mod tests {
    use brewdb_core::common::RequestContext;
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{JobId, StageId, TaskId};
    use brewdb_execution::plan::{StageGraph, StageKind, StagePlan, TaskPlan};

    use crate::planning::{BuildPlan, PlanSpec};
    use crate::scheduler::{SchedulingSnapshot, WorkerSlot};
    use brewdb_execution::task::TaskDependency;

    use super::{AdmissionService, AdmitRequest, DirectAdmissionService, StartDispatch};

    fn sample_graph(job_id: JobId) -> StageGraph {
        let stage_id = StageId::parse_str("550e8400-e29b-41d4-a716-446655440701").unwrap();

        StageGraph::new(
            job_id,
            vec![StagePlan {
                stage_id: stage_id.clone(),
                kind: StageKind::Exchange,
                boundary: Some(BoundaryKind::Exchange),
            }],
        )
        .with_tasks(vec![TaskPlan {
            task_id: TaskId::parse_str("550e8400-e29b-41d4-a716-446655440702").unwrap(),
            stage_id,
            partition_id: 0,
            dependencies: vec![TaskDependency {
                upstream_stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440703")
                    .unwrap(),
                upstream_task_id: None,
                partition_id: 0,
            }],
        }])
    }

    #[test]
    fn admission_bootstrap_hands_off_job_and_graph_registration() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440700").unwrap();
        let command = AdmitRequest {
            build_plan: BuildPlan {
                job_id: job_id.clone(),
                request_context: RequestContext::new(),
                spec: PlanSpec::Query {
                    stage_graph: sample_graph(job_id.clone()),
                },
            },
        };

        let bootstrap = command.bootstrap().unwrap();

        assert_eq!(bootstrap.job_record.job_id, job_id);
        assert_eq!(bootstrap.orchestration_plan.job_id, job_id);
        assert_eq!(bootstrap.stage_ids().len(), 1);
        assert!(bootstrap.register_execution_graph.is_some());
    }

    #[test]
    fn start_dispatch_converts_into_dispatch_wave() {
        let command = StartDispatch {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440710").unwrap(),
            snapshot: SchedulingSnapshot::default(),
            workers: vec![WorkerSlot {
                worker_id: "worker-a".to_owned(),
                available_slots: 4,
            }],
        };

        let wave = command.into_dispatch_wave();

        assert_eq!(wave.workers.len(), 1);
        assert_eq!(wave.workers[0].available_slots, 4);
    }

    #[test]
    fn direct_admission_service_delegates_bootstrap() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440720").unwrap();
        let service = DirectAdmissionService;

        let bootstrap = service
            .admit_request(AdmitRequest {
                build_plan: BuildPlan {
                    job_id: job_id.clone(),
                    request_context: RequestContext::new(),
                    spec: PlanSpec::Query {
                        stage_graph: sample_graph(job_id.clone()),
                    },
                },
            })
            .unwrap();

        assert_eq!(bootstrap.job_record.job_id, job_id);
        assert!(bootstrap.register_execution_graph.is_some());
    }
}
