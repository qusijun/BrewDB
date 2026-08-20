use std::sync::Arc;

use datafusion_common::Column;
use datafusion_expr::{Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan};

use crate::catalog::TableCatalogEntry;
use crate::common::context::QueryContext;
use crate::planner::distributed::exchange::{ExchangeNode, PartitioningScheme, RemoteSourceNode};
use crate::planner::distributed::split::{TableScanSplit, TableScanSplitGroup};
use crate::planner::errors::{map_df_plan_error, PlannerError};
use crate::planner::logical::command::{
    command_plan, command_tag, returns_rows, CommandPlan, CommandTag,
};
use crate::planner::logical::table_source::DefaultTableSource;
use crate::planner::logical::LogicalOptimizer;
use crate::storage::StorageEngine;

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
    pub command_tag: CommandTag,
    pub returns_rows: bool,
    pub fragments: Vec<PlanFragment>,
    pub fragment_scan_splits: Vec<FragmentScanSplits>,
    pub exchanges: Vec<ExchangeNode>,
}

#[derive(Debug, Default)]
pub struct DistributedFragmentPlanner;

pub trait FragmentPlanner: Send + Sync {
    fn plan_fragments(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        storage: Arc<dyn StorageEngine>,
    ) -> Result<DistributedFragmentPlan, PlannerError>;
}

impl DistributedFragmentPlanner {
    pub fn build(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        storage: Arc<dyn StorageEngine>,
    ) -> Result<DistributedFragmentPlan, PlannerError> {
        let optimized = optimize_logical_plan(logical_plan.clone())?;
        if let Some(command) = command_plan(&optimized) {
            return Ok(DistributedFragmentPlan {
                query_context,
                root: DistributedPlanRoot::Command(command),
                table_catalogs: collect_table_catalogs(&optimized),
                command_tag: command_tag(&optimized),
                returns_rows: returns_rows(&optimized),
                fragments: Vec::new(),
                fragment_scan_splits: Vec::new(),
                exchanges: Vec::new(),
            });
        }

        build_distributed_plan_with_context(
            optimized,
            query_context,
            storage,
            collect_table_catalogs(&logical_plan),
            command_tag(&logical_plan),
            returns_rows(&logical_plan),
        )
    }
}

impl FragmentPlanner for DistributedFragmentPlanner {
    fn plan_fragments(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        storage: Arc<dyn StorageEngine>,
    ) -> Result<DistributedFragmentPlan, PlannerError> {
        build_distributed_plan_with_context(
            logical_plan.clone(),
            query_context,
            storage,
            collect_table_catalogs(&logical_plan),
            command_tag(&logical_plan),
            returns_rows(&logical_plan),
        )
    }
}

#[derive(Debug, Default)]
pub struct StandaloneFragmentPlanner;

impl FragmentPlanner for StandaloneFragmentPlanner {
    fn plan_fragments(
        &self,
        query_context: QueryContext,
        logical_plan: DataFusionLogicalPlan,
        storage: Arc<dyn StorageEngine>,
    ) -> Result<DistributedFragmentPlan, PlannerError> {
        let root_fragment_id = PlanFragmentId(0);
        let table_catalogs = collect_table_catalogs(&logical_plan);
        let fragment_scan_splits = {
            let table_scan_splits = collect_table_scan_splits(&logical_plan, storage.as_ref())?;
            if table_scan_splits.is_empty() {
                Vec::new()
            } else {
                vec![FragmentScanSplits {
                    fragment_id: root_fragment_id,
                    table_scan_splits,
                }]
            }
        };

        Ok(DistributedFragmentPlan {
            query_context,
            root: DistributedPlanRoot::Fragments,
            table_catalogs,
            command_tag: command_tag(&logical_plan),
            returns_rows: returns_rows(&logical_plan),
            fragments: vec![PlanFragment {
                fragment_id: root_fragment_id,
                kind: PlanFragmentKind::Root,
                root: Some(logical_plan.clone()),
                local_plan: Some(logical_plan),
            }],
            fragment_scan_splits,
            exchanges: Vec::new(),
        })
    }
}

fn build_distributed_plan_with_context(
    root: DataFusionLogicalPlan,
    query_context: QueryContext,
    storage: Arc<dyn StorageEngine>,
    table_catalogs: Vec<TableCatalogEntry>,
    command_tag: CommandTag,
    returns_rows: bool,
) -> Result<DistributedFragmentPlan, PlannerError> {
    let root_fragment_id = PlanFragmentId(0);
    DistributedPlanBuilder::new(
        root_fragment_id,
        query_context,
        storage,
        table_catalogs,
        command_tag,
        returns_rows,
    )
    .build(root)
}

fn optimize_logical_plan(
    root: DataFusionLogicalPlan,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    LogicalOptimizer::default()
        .optimize(root)
        .map_err(map_df_plan_error)
}

fn collect_table_catalogs(root: &DataFusionLogicalPlan) -> Vec<TableCatalogEntry> {
    let mut tables = Vec::new();
    collect_table_catalogs_into(root, &mut tables);
    tables
}

fn collect_table_catalogs_into(plan: &DataFusionLogicalPlan, tables: &mut Vec<TableCatalogEntry>) {
    match plan {
        DataFusionLogicalPlan::TableScan(scan) => {
            push_table_catalog(
                tables,
                scan.source
                    .downcast_ref::<DefaultTableSource>()
                    .map(DefaultTableSource::table),
            );
        }
        DataFusionLogicalPlan::Dml(dml) => {
            push_table_catalog(
                tables,
                dml.target
                    .downcast_ref::<DefaultTableSource>()
                    .map(DefaultTableSource::table),
            );
            collect_table_catalogs_into(dml.input.as_ref(), tables);
        }
        _ => {
            for input in plan.inputs() {
                collect_table_catalogs_into(input, tables);
            }
        }
    }
}

fn push_table_catalog(tables: &mut Vec<TableCatalogEntry>, table: Option<&TableCatalogEntry>) {
    let Some(table) = table else {
        return;
    };
    if !tables
        .iter()
        .any(|existing| existing.table_id == table.table_id)
    {
        tables.push(table.clone());
    }
}

struct DistributedPlanBuilder {
    root_fragment_id: PlanFragmentId,
    query_context: QueryContext,
    storage: Arc<dyn StorageEngine>,
    table_catalogs: Vec<TableCatalogEntry>,
    command_tag: CommandTag,
    returns_rows: bool,
    next_fragment_id: u32,
    fragments: Vec<PlanFragment>,
    fragment_scan_splits: Vec<FragmentScanSplits>,
    exchanges: Vec<ExchangeNode>,
}

impl DistributedPlanBuilder {
    fn new(
        root_fragment_id: PlanFragmentId,
        query_context: QueryContext,
        storage: Arc<dyn StorageEngine>,
        table_catalogs: Vec<TableCatalogEntry>,
        command_tag: CommandTag,
        returns_rows: bool,
    ) -> Self {
        Self {
            root_fragment_id,
            query_context,
            storage,
            table_catalogs,
            command_tag,
            returns_rows,
            next_fragment_id: root_fragment_id.0 + 1,
            fragments: vec![PlanFragment {
                fragment_id: root_fragment_id,
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: None,
            }],
            fragment_scan_splits: Vec::new(),
            exchanges: Vec::new(),
        }
    }

    fn build(
        mut self,
        root: DataFusionLogicalPlan,
    ) -> Result<DistributedFragmentPlan, PlannerError> {
        self.fragments[0].root = Some(root.clone());
        let rewritten = self.rewrite_plan(&root, self.root_fragment_id)?;
        self.push_fragment_scan_splits(self.root_fragment_id, &rewritten)?;
        self.fragments[0].local_plan = Some(rewritten.clone());
        self.fragments[0].root = Some(rewritten);
        Ok(DistributedFragmentPlan {
            query_context: self.query_context,
            root: DistributedPlanRoot::Fragments,
            table_catalogs: self.table_catalogs,
            command_tag: self.command_tag,
            returns_rows: self.returns_rows,
            fragments: self.fragments,
            fragment_scan_splits: self.fragment_scan_splits,
            exchanges: self.exchanges,
        })
    }

    fn rewrite_plan(
        &mut self,
        plan: &DataFusionLogicalPlan,
        current_fragment_id: PlanFragmentId,
    ) -> Result<DataFusionLogicalPlan, PlannerError> {
        Ok(match plan {
            DataFusionLogicalPlan::Aggregate(aggregate) => {
                let child_remote_source = self.split_input_fragment(
                    aggregate.input.as_ref(),
                    current_fragment_id,
                    exchange_for_aggregate(aggregate.group_expr.as_slice()),
                )?;
                rebuild_aggregate(aggregate, child_remote_source)?
            }
            DataFusionLogicalPlan::Join(join) => {
                let left_remote_source = self.split_input_fragment(
                    join.left.as_ref(),
                    current_fragment_id,
                    exchange_for_join_side(join, true),
                )?;
                let right_remote_source = self.split_input_fragment(
                    join.right.as_ref(),
                    current_fragment_id,
                    exchange_for_join_side(join, false),
                )?;
                rebuild_join(join, left_remote_source, right_remote_source)?
            }
            DataFusionLogicalPlan::Projection(projection) => {
                let input = self.rewrite_plan(projection.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Projection(
                    datafusion_expr::Projection::try_new_with_schema(
                        projection.expr.clone(),
                        Arc::new(input),
                        projection.schema.clone(),
                    )
                    .map_err(map_df_plan_error)?,
                )
            }
            DataFusionLogicalPlan::Filter(filter) => {
                let input = self.rewrite_plan(filter.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Filter(
                    datafusion_expr::Filter::try_new(filter.predicate.clone(), Arc::new(input))
                        .map_err(map_df_plan_error)?,
                )
            }
            DataFusionLogicalPlan::SubqueryAlias(alias) => {
                let input = self.rewrite_plan(alias.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::SubqueryAlias(
                    datafusion_expr::SubqueryAlias::try_new(Arc::new(input), alias.alias.clone())
                        .map_err(map_df_plan_error)?,
                )
            }
            DataFusionLogicalPlan::Sort(sort) => {
                let input = self.rewrite_plan(sort.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Sort(datafusion_expr::Sort {
                    expr: sort.expr.clone(),
                    input: Arc::new(input),
                    fetch: sort.fetch,
                })
            }
            DataFusionLogicalPlan::Limit(limit) => {
                let input = self.rewrite_plan(limit.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Limit(datafusion_expr::Limit {
                    skip: limit.skip.clone(),
                    fetch: limit.fetch.clone(),
                    input: Arc::new(input),
                })
            }
            DataFusionLogicalPlan::Repartition(repartition) => {
                let input = self.rewrite_plan(repartition.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Repartition(datafusion_expr::Repartition {
                    input: Arc::new(input),
                    partitioning_scheme: repartition.partitioning_scheme.clone(),
                })
            }
            DataFusionLogicalPlan::Window(window) => {
                let input = self.rewrite_plan(window.input.as_ref(), current_fragment_id)?;
                DataFusionLogicalPlan::Window(
                    datafusion_expr::Window::try_new_with_schema(
                        window.window_expr.clone(),
                        Arc::new(input),
                        window.schema.clone(),
                    )
                    .map_err(map_df_plan_error)?,
                )
            }
            DataFusionLogicalPlan::Union(union) => {
                let inputs = union
                    .inputs
                    .iter()
                    .map(|input| self.rewrite_plan(input.as_ref(), current_fragment_id))
                    .collect::<Result<Vec<_>, _>>()?;
                DataFusionLogicalPlan::Union(
                    datafusion_expr::Union::try_new_with_loose_types(
                        inputs.into_iter().map(Arc::new).collect(),
                    )
                    .map_err(map_df_plan_error)?,
                )
            }
            DataFusionLogicalPlan::Distinct(distinct) => match distinct {
                datafusion_expr::Distinct::All(input) => {
                    let input = self.rewrite_plan(input.as_ref(), current_fragment_id)?;
                    DataFusionLogicalPlan::Distinct(datafusion_expr::Distinct::All(Arc::new(input)))
                }
                datafusion_expr::Distinct::On(distinct_on) => {
                    let input =
                        self.rewrite_plan(distinct_on.input.as_ref(), current_fragment_id)?;
                    DataFusionLogicalPlan::Distinct(datafusion_expr::Distinct::On(
                        datafusion_expr::DistinctOn::try_new(
                            distinct_on.on_expr.clone(),
                            distinct_on.select_expr.clone(),
                            distinct_on.sort_expr.clone(),
                            Arc::new(input),
                        )
                        .map_err(map_df_plan_error)?,
                    ))
                }
            },
            _ => plan.clone(),
        })
    }

    fn split_input_fragment(
        &mut self,
        source_plan: &DataFusionLogicalPlan,
        target_fragment_id: PlanFragmentId,
        exchange: ExchangeNode,
    ) -> Result<DataFusionLogicalPlan, PlannerError> {
        let source_fragment_id = self.next_fragment_id();
        let rewritten_source_plan = self.rewrite_plan(source_plan, source_fragment_id)?;
        self.push_fragment(source_fragment_id, source_plan, rewritten_source_plan)?;
        self.push_exchange(
            source_fragment_id,
            target_fragment_id,
            source_plan,
            exchange,
        );
        Ok(RemoteSourceNode::plan(
            source_fragment_id,
            source_plan.schema().clone(),
        ))
    }

    fn next_fragment_id(&mut self) -> PlanFragmentId {
        let fragment_id = PlanFragmentId(self.next_fragment_id);
        self.next_fragment_id += 1;
        fragment_id
    }

    fn push_fragment(
        &mut self,
        fragment_id: PlanFragmentId,
        original_plan: &DataFusionLogicalPlan,
        local_plan: DataFusionLogicalPlan,
    ) -> Result<(), PlannerError> {
        self.push_fragment_scan_splits(fragment_id, &local_plan)?;
        self.fragments.push(PlanFragment {
            fragment_id,
            kind: fragment_kind_for_plan(original_plan),
            root: None,
            local_plan: Some(local_plan),
        });
        Ok(())
    }

    fn push_fragment_scan_splits(
        &mut self,
        fragment_id: PlanFragmentId,
        local_plan: &DataFusionLogicalPlan,
    ) -> Result<(), PlannerError> {
        let table_scan_splits = collect_table_scan_splits(local_plan, self.storage.as_ref())?;
        if !table_scan_splits.is_empty() {
            self.fragment_scan_splits.push(FragmentScanSplits {
                fragment_id,
                table_scan_splits,
            });
        }
        Ok(())
    }

    fn push_exchange(
        &mut self,
        source_fragment_id: PlanFragmentId,
        target_fragment_id: PlanFragmentId,
        source_plan: &DataFusionLogicalPlan,
        exchange: ExchangeNode,
    ) {
        self.exchanges.push(ExchangeNode::new(
            source_fragment_id,
            target_fragment_id,
            exchange.scope,
            exchange.exchange_type,
            exchange
                .partitioning_scheme
                .with_output_layout(output_layout_for_plan(source_plan)),
        ));
    }
}

fn fragment_kind_for_plan(plan: &DataFusionLogicalPlan) -> PlanFragmentKind {
    if contains_remote_source(plan) {
        PlanFragmentKind::Intermediate
    } else {
        PlanFragmentKind::Source
    }
}

fn contains_remote_source(plan: &DataFusionLogicalPlan) -> bool {
    if let DataFusionLogicalPlan::Extension(extension) = plan {
        if extension
            .node
            .as_any()
            .downcast_ref::<RemoteSourceNode>()
            .is_some()
        {
            return true;
        }
    }

    plan.inputs()
        .iter()
        .any(|input| contains_remote_source(input))
}

fn output_layout_for_plan(plan: &DataFusionLogicalPlan) -> Vec<Column> {
    plan.schema().columns().to_vec()
}

fn collect_table_scan_splits(
    plan: &DataFusionLogicalPlan,
    storage: &dyn StorageEngine,
) -> Result<TableScanSplitGroup, PlannerError> {
    let mut splits = Vec::new();
    collect_table_scan_splits_into(plan, storage, &mut splits)?;
    Ok(TableScanSplitGroup::new(splits))
}

fn collect_table_scan_splits_into(
    plan: &DataFusionLogicalPlan,
    storage: &dyn StorageEngine,
    splits: &mut Vec<TableScanSplit>,
) -> Result<(), PlannerError> {
    match plan {
        DataFusionLogicalPlan::TableScan(scan) => {
            if let Some(table_source) = scan.source.downcast_ref::<DefaultTableSource>() {
                let table_engine = match table_source.table_engine() {
                    Some(table_engine) => Arc::clone(table_engine),
                    None => storage.table_engine(table_source.table()).map_err(|err| {
                        PlannerError::InvalidPlan {
                            reason: err.to_string(),
                        }
                    })?,
                };
                let planned_splits =
                    table_engine
                        .plan_scan(scan)
                        .map_err(|err| PlannerError::InvalidPlan {
                            reason: err.to_string(),
                        })?;
                splits.extend(planned_splits.splits);
            } else {
                splits.push(TableScanSplit::new(scan.table_name.to_string(), 0));
            }
        }
        _ => {
            for input in plan.inputs() {
                collect_table_scan_splits_into(input, storage, splits)?;
            }
        }
    }
    Ok(())
}

fn exchange_for_aggregate(group_expr: &[DataFusionExpr]) -> ExchangeNode {
    if group_expr.is_empty() {
        ExchangeNode::gather(PlanFragmentId(0), PlanFragmentId(0))
    } else {
        let partition_keys = group_expr
            .iter()
            .flat_map(|expr| expr.column_refs().into_iter().cloned())
            .collect::<Vec<_>>();
        ExchangeNode::repartition(
            PlanFragmentId(0),
            PlanFragmentId(0),
            PartitioningScheme::hash(partition_keys),
        )
    }
}

fn exchange_for_join_side(join: &datafusion_expr::logical_plan::Join, left: bool) -> ExchangeNode {
    if join.on.is_empty() {
        ExchangeNode::replicate(PlanFragmentId(0), PlanFragmentId(0))
    } else {
        let partition_keys = join
            .on
            .iter()
            .flat_map(|(lhs, rhs)| {
                if left {
                    lhs.column_refs().into_iter().cloned().collect::<Vec<_>>()
                } else {
                    rhs.column_refs().into_iter().cloned().collect::<Vec<_>>()
                }
            })
            .collect::<Vec<_>>();
        ExchangeNode::repartition(
            PlanFragmentId(0),
            PlanFragmentId(0),
            PartitioningScheme::hash(partition_keys),
        )
    }
}

fn rebuild_aggregate(
    aggregate: &datafusion_expr::Aggregate,
    input: DataFusionLogicalPlan,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    Ok(DataFusionLogicalPlan::Aggregate(
        datafusion_expr::Aggregate::try_new_with_schema(
            Arc::new(input),
            aggregate.group_expr.clone(),
            aggregate.aggr_expr.clone(),
            aggregate.schema.clone(),
        )
        .map_err(map_df_plan_error)?,
    ))
}

fn rebuild_join(
    join: &datafusion_expr::Join,
    left: DataFusionLogicalPlan,
    right: DataFusionLogicalPlan,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    Ok(DataFusionLogicalPlan::Join(
        datafusion_expr::Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on.clone(),
            join.filter.clone(),
            join.join_type.clone(),
            join.join_constraint.clone(),
            join.null_equality,
            join.null_aware,
        )
        .map_err(map_df_plan_error)?,
    ))
}
