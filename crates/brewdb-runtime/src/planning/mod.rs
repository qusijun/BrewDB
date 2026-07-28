//! Runtime planning from intents into orchestration plans.

use brewdb_core::common::RequestContext;
use brewdb_core::ids::{ArtifactId, JobId, TableId};
use brewdb_core::txn::ResourceLane;
use brewdb_execution::plan::StageGraph;

use crate::errors::RuntimeError;
use crate::jobs::SubmitJob;

/// Runtime-visible job families that drive orchestration shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JobKind {
    Query,
    Append,
    RewriteMutation,
    Maintenance,
    Ddl,
}

/// Commit/finalization work that follows execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitPlan {
    pub target_table_id: TableId,
    pub lane: ResourceLane,
    pub staged_artifact_ids: Vec<ArtifactId>,
    pub requires_txn: bool,
}

/// Runtime-owned orchestration plan assembled before dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrchestrationPlan {
    pub job_id: JobId,
    pub job_kind: JobKind,
    pub target_table_id: Option<TableId>,
    pub lane: Option<ResourceLane>,
    pub stage_graph: Option<StageGraph>,
    pub commit_plan: Option<CommitPlan>,
}

impl OrchestrationPlan {
    pub fn query(job_id: JobId, stage_graph: StageGraph) -> Self {
        Self {
            job_id,
            job_kind: JobKind::Query,
            target_table_id: None,
            lane: None,
            stage_graph: Some(stage_graph),
            commit_plan: None,
        }
    }

    pub fn finalize_after_execution(
        job_id: JobId,
        job_kind: JobKind,
        stage_graph: StageGraph,
        commit_plan: CommitPlan,
    ) -> Self {
        Self {
            job_id,
            job_kind,
            target_table_id: Some(commit_plan.target_table_id.clone()),
            lane: Some(commit_plan.lane),
            stage_graph: Some(stage_graph),
            commit_plan: Some(commit_plan),
        }
    }

    pub fn requires_txn(&self) -> bool {
        self.commit_plan
            .as_ref()
            .map(|plan| plan.requires_txn)
            .unwrap_or(false)
    }
}

/// Runtime planning request assembled from upper-layer intent and lower-layer shape data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildPlan {
    pub job_id: JobId,
    pub request_context: RequestContext,
    pub spec: PlanSpec,
}

impl BuildPlan {
    pub fn into_submit_job(&self) -> SubmitJob {
        SubmitJob {
            job_id: self.job_id.clone(),
            target_table_id: self.spec.target_table_id(),
            lane: self.spec.lane(),
            request_context: self.request_context.clone(),
        }
    }

    pub fn build(self) -> Result<OrchestrationPlan, RuntimeError> {
        let job_id = self.job_id;

        match self.spec {
            PlanSpec::Query { stage_graph } => {
                ensure_graph_belongs_to_job(&job_id, &stage_graph)?;
                ensure_graph_not_empty(&stage_graph)?;
                Ok(OrchestrationPlan::query(job_id, stage_graph))
            }
            PlanSpec::FinalizeAfterExecution {
                job_kind,
                target_table_id,
                lane,
                stage_graph,
                staged_artifact_ids,
                requires_txn,
            } => {
                ensure_graph_belongs_to_job(&job_id, &stage_graph)?;
                ensure_graph_not_empty(&stage_graph)?;
                ensure_finalize_job_kind(job_kind)?;

                if staged_artifact_ids.is_empty() {
                    return Err(RuntimeError::MissingField {
                        entity: "orchestration_plan",
                        field: "staged_artifact_ids",
                    });
                }

                let commit_plan = CommitPlan {
                    target_table_id,
                    lane,
                    staged_artifact_ids,
                    requires_txn,
                };

                Ok(OrchestrationPlan::finalize_after_execution(
                    job_id,
                    job_kind,
                    stage_graph,
                    commit_plan,
                ))
            }
        }
    }
}

/// Runtime planning inputs normalized away from SQL or RPC details.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanSpec {
    Query {
        stage_graph: StageGraph,
    },
    FinalizeAfterExecution {
        job_kind: JobKind,
        target_table_id: TableId,
        lane: ResourceLane,
        stage_graph: StageGraph,
        staged_artifact_ids: Vec<ArtifactId>,
        requires_txn: bool,
    },
}

impl PlanSpec {
    pub fn target_table_id(&self) -> Option<TableId> {
        match self {
            Self::Query { .. } => None,
            Self::FinalizeAfterExecution {
                target_table_id, ..
            } => Some(target_table_id.clone()),
        }
    }

    pub fn lane(&self) -> Option<ResourceLane> {
        match self {
            Self::Query { .. } => None,
            Self::FinalizeAfterExecution { lane, .. } => Some(*lane),
        }
    }
}

pub trait PlanningService {
    fn build_plan(&self, command: BuildPlan) -> Result<OrchestrationPlan, RuntimeError>;
}

fn ensure_graph_not_empty(stage_graph: &StageGraph) -> Result<(), RuntimeError> {
    if stage_graph.is_empty() {
        return Err(RuntimeError::MissingField {
            entity: "orchestration_plan",
            field: "stage_graph.stages",
        });
    }

    Ok(())
}

fn ensure_graph_belongs_to_job(
    job_id: &JobId,
    stage_graph: &StageGraph,
) -> Result<(), RuntimeError> {
    if &stage_graph.job_id != job_id {
        return Err(RuntimeError::StateConflict {
            entity: "orchestration_plan",
            reason: format!(
                "stage graph job_id {} does not match plan job_id {}",
                stage_graph.job_id, job_id
            ),
        });
    }

    Ok(())
}

fn ensure_finalize_job_kind(job_kind: JobKind) -> Result<(), RuntimeError> {
    if matches!(job_kind, JobKind::Query) {
        return Err(RuntimeError::StateConflict {
            entity: "orchestration_plan",
            reason: "query jobs cannot carry a commit plan".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use brewdb_core::common::RequestContext;
    use brewdb_core::execution::BoundaryKind;
    use brewdb_core::ids::{ArtifactId, JobId, StageId, TableId};
    use brewdb_core::txn::ResourceLane;
    use brewdb_execution::plan::{StageGraph, StageKind, StagePlan};

    use super::{BuildPlan, CommitPlan, JobKind, OrchestrationPlan, PlanSpec};

    fn sample_graph(job_id: JobId) -> StageGraph {
        StageGraph::new(
            job_id,
            vec![StagePlan {
                stage_id: StageId::parse_str("550e8400-e29b-41d4-a716-446655440311").unwrap(),
                kind: StageKind::Materialize,
                boundary: Some(BoundaryKind::Materialization),
            }],
        )
    }

    #[test]
    fn query_plan_has_no_commit_phase() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440310").unwrap();
        let plan = OrchestrationPlan::query(job_id.clone(), sample_graph(job_id.clone()));

        assert_eq!(plan.job_kind, JobKind::Query);
        assert!(plan.target_table_id.is_none());
        assert!(plan.lane.is_none());
        assert!(plan.commit_plan.is_none());
        assert!(!plan.requires_txn());
    }

    #[test]
    fn append_plan_requires_txn() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440320").unwrap();
        let commit_plan = CommitPlan {
            target_table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655440321").unwrap(),
            lane: ResourceLane::Mutation,
            staged_artifact_ids: vec![
                ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655440322").unwrap(),
            ],
            requires_txn: true,
        };
        let expected_table_id = commit_plan.target_table_id.clone();
        let plan = OrchestrationPlan::finalize_after_execution(
            job_id.clone(),
            JobKind::Append,
            sample_graph(job_id),
            commit_plan,
        );

        assert_eq!(plan.job_kind, JobKind::Append);
        assert_eq!(plan.target_table_id, expected_table_id.into());
        assert_eq!(plan.lane, Some(ResourceLane::Mutation));
        assert!(plan.commit_plan.is_some());
        assert!(plan.requires_txn());
    }

    #[test]
    fn build_plan_produces_submit_job_shape() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440330").unwrap();
        let table_id = TableId::parse_str("550e8400-e29b-41d4-a716-446655440331").unwrap();
        let command = BuildPlan {
            job_id: job_id.clone(),
            request_context: RequestContext::new(),
            spec: PlanSpec::FinalizeAfterExecution {
                job_kind: JobKind::Append,
                target_table_id: table_id.clone(),
                lane: ResourceLane::Mutation,
                stage_graph: sample_graph(job_id),
                staged_artifact_ids: vec![
                    ArtifactId::parse_str("550e8400-e29b-41d4-a716-446655440332").unwrap(),
                ],
                requires_txn: true,
            },
        };

        let submit_job = command.into_submit_job();

        assert_eq!(submit_job.target_table_id, Some(table_id));
        assert_eq!(submit_job.lane, Some(ResourceLane::Mutation));
    }

    #[test]
    fn build_plan_rejects_job_graph_mismatch() {
        let command = BuildPlan {
            job_id: JobId::parse_str("550e8400-e29b-41d4-a716-446655440340").unwrap(),
            request_context: RequestContext::new(),
            spec: PlanSpec::Query {
                stage_graph: sample_graph(
                    JobId::parse_str("550e8400-e29b-41d4-a716-446655440341").unwrap(),
                ),
            },
        };

        let error = command.build().unwrap_err();

        assert!(matches!(
            error,
            crate::errors::RuntimeError::StateConflict { .. }
        ));
    }

    #[test]
    fn build_plan_rejects_missing_artifacts_for_commit_path() {
        let job_id = JobId::parse_str("550e8400-e29b-41d4-a716-446655440350").unwrap();
        let command = BuildPlan {
            job_id: job_id.clone(),
            request_context: RequestContext::new(),
            spec: PlanSpec::FinalizeAfterExecution {
                job_kind: JobKind::Append,
                target_table_id: TableId::parse_str("550e8400-e29b-41d4-a716-446655440351")
                    .unwrap(),
                lane: ResourceLane::Mutation,
                stage_graph: sample_graph(job_id),
                staged_artifact_ids: Vec::new(),
                requires_txn: true,
            },
        };

        let error = command.build().unwrap_err();

        assert!(matches!(
            error,
            crate::errors::RuntimeError::MissingField { .. }
        ));
    }
}
