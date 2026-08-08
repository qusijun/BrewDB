//! Distributed execution plan skeleton.

use brewdb_common::runtime::QueryContext;
use datafusion_expr::LogicalPlan as DataFusionLogicalPlan;

use crate::exchange::ExchangeNode;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlanStageId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlanFragmentId {
    pub stage_id: PlanStageId,
    pub fragment_ordinal: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanFragmentKind {
    Source,
    Intermediate,
    Root,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanFragment {
    pub fragment_id: PlanFragmentId,
    pub kind: PlanFragmentKind,
    pub root: Option<crate::logical::LogicalPlan>,
    pub local_plan: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedPlan {
    pub query_context: QueryContext,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<ExchangeNode>,
}
