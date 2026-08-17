//! Distributed execution plan skeleton.

use crate::catalog::TableCatalogEntry;
use crate::common::runtime::QueryContext;
use datafusion_expr::{
    DdlStatement, LogicalPlan as DataFusionLogicalPlan, Statement as DataFusionStatement,
};

use crate::planner::logical::plan::LogicalPlanNode;

use super::exchange::ExchangeNode;
use super::split::TableScanSplitGroup;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlanFragmentId(pub u32);

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
pub struct FragmentScanSplits {
    pub fragment_id: PlanFragmentId,
    pub table_scan_splits: TableScanSplitGroup,
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
/// Planner output that describes fragment boundaries, exchange topology, and
/// scan split candidates.
///
/// Runtime scheduling state such as worker placement, fragment instances, and
/// concrete scan split assignment belongs to the runtime execution graph, not
/// here.
pub struct DistributedFragmentPlan {
    pub query_context: QueryContext,
    pub root: DistributedPlanRoot,
    pub table_catalogs: Vec<TableCatalogEntry>,
    pub command_tag: String,
    pub returns_rows: bool,
    pub fragments: Vec<PlanFragment>,
    pub fragment_scan_splits: Vec<FragmentScanSplits>,
    pub exchanges: Vec<ExchangeNode>,
}
