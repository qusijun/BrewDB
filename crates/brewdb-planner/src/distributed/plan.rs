//! Distributed execution plan skeleton.

use brewdb_catalog::TableCatalogEntry;
use brewdb_common::runtime::QueryContext;
use datafusion_expr::{
    DdlStatement, LogicalPlan as DataFusionLogicalPlan, Statement as DataFusionStatement,
};

use crate::logical::plan::LogicalPlanNode;

use super::exchange::ExchangeNode;

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
    pub root: Option<DataFusionLogicalPlan>,
    pub local_plan: Option<DataFusionLogicalPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandPlan {
    Ddl(DdlStatement),
    Extension(LogicalPlanNode),
    Statement(DataFusionStatement),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DistributedPlanRoot {
    Fragments,
    Command(CommandPlan),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedPhysicalPlan {
    pub query_context: QueryContext,
    pub root: DistributedPlanRoot,
    pub table_catalogs: Vec<TableCatalogEntry>,
    pub command_tag: String,
    pub returns_rows: bool,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<ExchangeNode>,
}
