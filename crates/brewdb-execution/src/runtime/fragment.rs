//! Worker-executable fragment units.

use crate::catalog::TableCatalogEntry;
use crate::common::context::QueryContext;
use crate::planner::distributed::{PlanFragment, PlanFragmentId};
use crate::runtime::exchange::ExchangeChannelDescriptor;
use crate::storage::TableScanSplit;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionFragment {
    pub fragment: PlanFragment,
}

impl ExecutionFragment {
    pub fn new(fragment: PlanFragment) -> Self {
        Self { fragment }
    }

    pub fn fragment_id(&self) -> PlanFragmentId {
        self.fragment.fragment_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FragmentInstance {
    pub instance_id: Uuid,
    pub execution_fragment: Box<ExecutionFragment>,
    pub worker_id: Uuid,
    pub endpoint: String,
    pub table_scan_split: Option<TableScanSplit>,
    pub query_context: QueryContext,
    pub table_catalogs: Vec<TableCatalogEntry>,
    pub exchange_inputs: Vec<ExchangeChannelDescriptor>,
    pub exchange_outputs: Vec<ExchangeChannelDescriptor>,
}

impl FragmentInstance {
    pub fn scheduled(
        instance_id: Uuid,
        execution_fragment: ExecutionFragment,
        worker_id: Uuid,
        endpoint: impl Into<String>,
        table_scan_split: Option<TableScanSplit>,
    ) -> Self {
        Self {
            instance_id,
            execution_fragment: Box::new(execution_fragment),
            worker_id,
            endpoint: endpoint.into(),
            table_scan_split,
            query_context: QueryContext::for_test(Uuid::nil()),
            table_catalogs: vec![],
            exchange_inputs: vec![],
            exchange_outputs: vec![],
        }
    }

    pub fn fragment(&self) -> &PlanFragment {
        &self.execution_fragment.fragment
    }

    pub fn fragment_id(&self) -> PlanFragmentId {
        self.execution_fragment.fragment_id()
    }
}
