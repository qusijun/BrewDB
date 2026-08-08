//! Distributed exchange contracts.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_expr::UserDefinedLogicalNodeCore;
use datafusion_expr::{Expr as DataFusionExpr, Extension, LogicalPlan as DataFusionLogicalPlan};

use crate::plan::PlanFragmentId;
use datafusion_common::Column;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeScope {
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeType {
    Gather,
    Repartition,
    Replicate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitioningScheme {
    pub partition_keys: Vec<Column>,
    pub output_layout: Vec<Column>,
}

impl PartitioningScheme {
    pub fn single() -> Self {
        Self {
            partition_keys: Vec::new(),
            output_layout: Vec::new(),
        }
    }

    pub fn hash(partition_keys: impl IntoIterator<Item = Column>) -> Self {
        Self {
            partition_keys: partition_keys.into_iter().collect(),
            output_layout: Vec::new(),
        }
    }

    pub fn with_output_layout(mut self, output_layout: impl IntoIterator<Item = Column>) -> Self {
        self.output_layout = output_layout.into_iter().collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeNode {
    pub source_fragment_id: PlanFragmentId,
    pub target_fragment_id: PlanFragmentId,
    pub scope: ExchangeScope,
    pub exchange_type: ExchangeType,
    pub partitioning_scheme: PartitioningScheme,
}

impl ExchangeNode {
    pub fn new(
        source_fragment_id: PlanFragmentId,
        target_fragment_id: PlanFragmentId,
        scope: ExchangeScope,
        exchange_type: ExchangeType,
        partitioning_scheme: PartitioningScheme,
    ) -> Self {
        Self {
            source_fragment_id,
            target_fragment_id,
            scope,
            exchange_type,
            partitioning_scheme,
        }
    }

    pub fn gather(source_fragment_id: PlanFragmentId, target_fragment_id: PlanFragmentId) -> Self {
        Self::new(
            source_fragment_id,
            target_fragment_id,
            ExchangeScope::Remote,
            ExchangeType::Gather,
            PartitioningScheme::single(),
        )
    }

    pub fn repartition(
        source_fragment_id: PlanFragmentId,
        target_fragment_id: PlanFragmentId,
        partitioning_scheme: PartitioningScheme,
    ) -> Self {
        Self::new(
            source_fragment_id,
            target_fragment_id,
            ExchangeScope::Remote,
            ExchangeType::Repartition,
            partitioning_scheme,
        )
    }

    pub fn replicate(
        source_fragment_id: PlanFragmentId,
        target_fragment_id: PlanFragmentId,
    ) -> Self {
        Self::new(
            source_fragment_id,
            target_fragment_id,
            ExchangeScope::Remote,
            ExchangeType::Replicate,
            PartitioningScheme::single(),
        )
    }

    pub fn with_partitioning_scheme(mut self, partitioning_scheme: PartitioningScheme) -> Self {
        self.partitioning_scheme = partitioning_scheme;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSourceNode {
    pub source_fragment_ids: Vec<PlanFragmentId>,
    pub schema: DFSchemaRef,
}

impl Hash for RemoteSourceNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_fragment_ids.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for RemoteSourceNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl RemoteSourceNode {
    pub fn new(source_fragment_ids: Vec<PlanFragmentId>, schema: DFSchemaRef) -> Self {
        Self {
            source_fragment_ids,
            schema,
        }
    }

    pub fn plan(source_fragment_id: PlanFragmentId, schema: DFSchemaRef) -> DataFusionLogicalPlan {
        DataFusionLogicalPlan::Extension(Extension {
            node: Arc::new(Self::new(vec![source_fragment_id], schema)),
        })
    }
}

impl UserDefinedLogicalNodeCore for RemoteSourceNode {
    fn name(&self) -> &str {
        "RemoteSource"
    }

    fn inputs(&self) -> Vec<&DataFusionLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<DataFusionExpr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "RemoteSource: sourceFragmentIds={:?}",
            self.source_fragment_ids
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<DataFusionExpr>,
        inputs: Vec<DataFusionLogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        assert!(
            inputs.is_empty(),
            "exchange placeholder must not have inputs"
        );
        Ok(Self {
            source_fragment_ids: self.source_fragment_ids.clone(),
            schema: Arc::clone(&self.schema),
        })
    }
}
