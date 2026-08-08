//! Minimal distributed planner scaffold.

use std::sync::Arc;

use crate::errors::PlannerError;
use crate::exchange::{ExchangeNode, PartitioningScheme, RemoteSourceNode};
use crate::logical::{
    DeletePlanRoot, InsertPlanRoot, LogicalPlan, MergePlanRoot, QueryExpression, QueryGroupBy,
    QueryPlanRoot, UpdatePlanRoot,
};
use crate::plan::{DistributedPlan, PlanFragment, PlanFragmentId, PlanFragmentKind, PlanStageId};
use brewdb_catalog::TableCatalogEntry;
use brewdb_common::runtime::QueryContext;
use brewdb_sql::{
    BoundDeleteStatement, BoundInsertStatement, BoundMergeStatement, BoundPlanStatement,
    BoundQueryStatement, BoundUpdateStatement,
};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction, WildcardOptions};
use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
use datafusion_expr::registry::{FunctionRegistry, MemoryFunctionRegistry};
use datafusion_expr::{
    BinaryExpr, Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
    Operator as DataFusionOperator, TableSource, col, lit,
};
use datafusion_functions as datafusion_scalar_functions;
use datafusion_functions_aggregate as datafusion_aggregate_functions;
use datafusion_optimizer::{Optimizer, OptimizerContext};
use datafusion_sql::sqlparser::ast::{
    BinaryOperator as AstBinaryOperator, DuplicateTreatment, Expr as AstExpr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Join, JoinConstraint,
    JoinOperator as AstJoinOperator, Query, Select, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, Statement as AstStatement, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator as AstUnaryOperator, Value,
};

#[derive(Debug)]
struct JoinCondition {
    left_keys: Vec<Column>,
    right_keys: Vec<Column>,
    filter: Option<DataFusionExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DistributedPlannerRequest {
    pub query_context: QueryContext,
    pub statement: BoundPlanStatement,
}

#[derive(Debug)]
pub struct DistributedPlanner {
    function_registry: MemoryFunctionRegistry,
}

impl Default for DistributedPlanner {
    fn default() -> Self {
        Self {
            function_registry: default_function_registry(),
        }
    }
}

impl DistributedPlanner {
    pub fn build(
        &self,
        request: DistributedPlannerRequest,
    ) -> Result<DistributedPlan, PlannerError> {
        build_distributed_plan_with_context(
            optimize_logical_plan(bind_statement_to_logical(
                request.statement,
                &self.function_registry,
            )?)?,
            request.query_context,
        )
    }
}

fn build_distributed_plan_with_context(
    root: LogicalPlan,
    query_context: QueryContext,
) -> Result<DistributedPlan, PlannerError> {
    let root_fragment_id = PlanFragmentId {
        stage_id: PlanStageId(0),
        fragment_ordinal: 0,
    };
    DistributedPlanBuilder::new(root_fragment_id, query_context).build(root)
}

fn optimize_logical_plan(root: LogicalPlan) -> Result<LogicalPlan, PlannerError> {
    Ok(match root {
        LogicalPlan::Query(mut query_root) => {
            if let Some(input) = query_root.input.take() {
                let optimizer = Optimizer::new();
                query_root.input = Some(
                    optimizer
                        .optimize(input.clone(), &OptimizerContext::new(), |_, _| {})
                        .unwrap_or(input),
                );
            }
            LogicalPlan::Query(query_root)
        }
        other => other,
    })
}

struct DistributedPlanBuilder {
    root_fragment_id: PlanFragmentId,
    query_context: QueryContext,
    next_stage_id: u32,
    fragments: Vec<PlanFragment>,
    exchanges: Vec<ExchangeNode>,
}

impl DistributedPlanBuilder {
    fn new(root_fragment_id: PlanFragmentId, query_context: QueryContext) -> Self {
        Self {
            root_fragment_id,
            query_context,
            next_stage_id: root_fragment_id.stage_id.0 + 1,
            fragments: vec![PlanFragment {
                fragment_id: root_fragment_id,
                kind: PlanFragmentKind::Root,
                root: None,
                local_plan: None,
            }],
            exchanges: Vec::new(),
        }
    }

    fn build(mut self, root: LogicalPlan) -> Result<DistributedPlan, PlannerError> {
        self.fragments[0].root = Some(root.clone());
        if let LogicalPlan::Query(query_root) = &root
            && let Some(plan) = query_root.input.as_ref()
        {
            let rewritten = self.rewrite_plan(plan, self.root_fragment_id)?;
            self.fragments[0].local_plan = Some(rewritten.clone());
            self.fragments[0].root = Some(LogicalPlan::Query(QueryPlanRoot {
                tables: query_root.tables.clone(),
                query: query_root.query.clone(),
                input: Some(rewritten),
            }));
        }
        Ok(DistributedPlan {
            query_context: self.query_context,
            fragments: self.fragments,
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
        self.push_fragment(source_fragment_id, source_plan, rewritten_source_plan);
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
        let fragment_id = PlanFragmentId {
            stage_id: PlanStageId(self.next_stage_id),
            fragment_ordinal: 0,
        };
        self.next_stage_id += 1;
        fragment_id
    }

    fn push_fragment(
        &mut self,
        fragment_id: PlanFragmentId,
        original_plan: &DataFusionLogicalPlan,
        local_plan: DataFusionLogicalPlan,
    ) {
        self.fragments.push(PlanFragment {
            fragment_id,
            kind: fragment_kind_for_plan(original_plan),
            root: None,
            local_plan: Some(local_plan),
        });
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
    if let DataFusionLogicalPlan::Extension(extension) = plan
        && extension
            .node
            .as_any()
            .downcast_ref::<RemoteSourceNode>()
            .is_some()
    {
        return true;
    }

    plan.inputs()
        .iter()
        .any(|input| contains_remote_source(input))
}

fn output_layout_for_plan(plan: &DataFusionLogicalPlan) -> Vec<Column> {
    plan.schema().columns().to_vec()
}

fn exchange_for_aggregate(group_expr: &[DataFusionExpr]) -> ExchangeNode {
    if group_expr.is_empty() {
        ExchangeNode::gather(
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
        )
    } else {
        let partition_keys = group_expr
            .iter()
            .flat_map(|expr| expr.column_refs().into_iter().cloned())
            .collect::<Vec<_>>();
        ExchangeNode::repartition(
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            PartitioningScheme::hash(partition_keys),
        )
    }
}

fn exchange_for_join_side(join: &datafusion_expr::logical_plan::Join, left: bool) -> ExchangeNode {
    if join.on.is_empty() {
        ExchangeNode::replicate(
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
        )
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
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
            PlanFragmentId {
                stage_id: PlanStageId(0),
                fragment_ordinal: 0,
            },
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

fn default_function_registry() -> MemoryFunctionRegistry {
    let mut registry = MemoryFunctionRegistry::new();
    datafusion_scalar_functions::register_all(&mut registry)
        .expect("default DataFusion scalar functions must register");
    datafusion_aggregate_functions::register_all(&mut registry)
        .expect("default DataFusion aggregate functions must register");
    registry
}

fn bind_statement_to_logical(
    statement: BoundPlanStatement,
    function_registry: &dyn FunctionRegistry,
) -> Result<LogicalPlan, PlannerError> {
    Ok(match statement {
        BoundPlanStatement::Query(statement) => {
            LogicalPlan::Query(bind_query_statement(statement, function_registry)?)
        }
        BoundPlanStatement::Insert(statement) => {
            LogicalPlan::Insert(bind_insert_statement(statement))
        }
        BoundPlanStatement::Delete(statement) => {
            LogicalPlan::Delete(bind_delete_statement(statement))
        }
        BoundPlanStatement::Update(statement) => {
            LogicalPlan::Update(bind_update_statement(statement))
        }
        BoundPlanStatement::Merge(statement) => LogicalPlan::Merge(bind_merge_statement(statement)),
    })
}

fn bind_query_statement(
    statement: BoundQueryStatement,
    function_registry: &dyn FunctionRegistry,
) -> Result<QueryPlanRoot, PlannerError> {
    let query = bind_query_expression(&statement.ast, function_registry)?;
    let input = Some(build_query_input(&statement, &query, function_registry)?);
    Ok(QueryPlanRoot {
        tables: statement.tables,
        query,
        input,
    })
}

fn bind_insert_statement(statement: BoundInsertStatement) -> InsertPlanRoot {
    InsertPlanRoot {
        target_table: statement.target_table,
        source_tables: statement.source_tables,
        ast: statement.ast,
        input: None,
    }
}

fn bind_delete_statement(statement: BoundDeleteStatement) -> DeletePlanRoot {
    DeletePlanRoot {
        target_table: statement.target_table,
        ast: statement.ast,
        input: None,
    }
}

fn bind_update_statement(statement: BoundUpdateStatement) -> UpdatePlanRoot {
    UpdatePlanRoot {
        target_table: statement.target_table,
        source_tables: statement.source_tables,
        ast: statement.ast,
        input: None,
    }
}

fn bind_merge_statement(statement: BoundMergeStatement) -> MergePlanRoot {
    MergePlanRoot {
        target_table: statement.target_table,
        source_tables: statement.source_tables,
        ast: statement.ast,
        input: None,
    }
}

fn bind_query_expression(
    statement: &AstStatement,
    function_registry: &dyn FunctionRegistry,
) -> Result<QueryExpression, PlannerError> {
    let AstStatement::Query(query) = statement else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{statement}`"),
        });
    };
    bind_select_query(query, function_registry)
}

fn build_query_input(
    statement: &BoundQueryStatement,
    query: &QueryExpression,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_from_input(statement, function_registry)?;
    if let Some(predicate) = &query.selection {
        input = LogicalPlanBuilder::from(input)
            .filter(predicate.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if needs_aggregate(query) {
        let aggregates = query
            .projection
            .iter()
            .filter(|expr| matches!(expr, DataFusionExpr::AggregateFunction(_)))
            .cloned()
            .collect::<Vec<_>>();
        let group_keys = match &query.group_by {
            QueryGroupBy::Expressions(expressions) => expressions.clone(),
            QueryGroupBy::None | QueryGroupBy::All => Vec::new(),
        };
        input = LogicalPlanBuilder::from(input)
            .aggregate(group_keys, aggregates)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
        if let Some(predicate) = &query.having {
            input = LogicalPlanBuilder::from(input)
                .filter(predicate.clone())
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
    }
    if projection_is_passthrough_wildcard(&query.projection) {
        return Ok(input);
    }
    LogicalPlanBuilder::from(input)
        .project(query.projection.clone())
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)
}

fn build_from_input(
    statement: &BoundQueryStatement,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let AstStatement::Query(query) = &statement.ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{}`", statement.ast),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{}`", query.body),
        });
    };
    if select.from.is_empty() {
        return Err(PlannerError::UnsupportedPlan {
            reason: "query without bound tables is not supported yet".to_string(),
        });
    }
    let mut inputs = select
        .from
        .iter()
        .map(|from| build_table_with_joins(from, &statement.tables, function_registry))
        .collect::<Result<Vec<_>, _>>()?;
    let mut input = inputs.remove(0);
    for next in inputs {
        input = LogicalPlanBuilder::from(input)
            .cross_join(next)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    Ok(input)
}

fn needs_aggregate(query: &QueryExpression) -> bool {
    match &query.group_by {
        QueryGroupBy::All | QueryGroupBy::Expressions(_) => true,
        QueryGroupBy::None => query
            .projection
            .iter()
            .any(|expr| matches!(expr, DataFusionExpr::AggregateFunction(_))),
    }
}

fn bind_select_query(
    query: &Query,
    function_registry: &dyn FunctionRegistry,
) -> Result<QueryExpression, PlannerError> {
    match query.body.as_ref() {
        SetExpr::Select(select) => bind_select(select, function_registry),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

fn bind_select(
    select: &Select,
    function_registry: &dyn FunctionRegistry,
) -> Result<QueryExpression, PlannerError> {
    if select.prewhere.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported select shape `{select}`"),
        });
    }
    Ok(QueryExpression {
        distinct: select.distinct.is_some(),
        projection: bind_projection(&select.projection, function_registry)?,
        selection: select
            .selection
            .as_ref()
            .map(|expr| bind_expr(expr, function_registry))
            .transpose()?,
        group_by: bind_group_by(&select.group_by, function_registry)?,
        having: select
            .having
            .as_ref()
            .map(|expr| bind_expr(expr, function_registry))
            .transpose()?,
    })
}

fn bind_projection(
    items: &[SelectItem],
    function_registry: &dyn FunctionRegistry,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    items
        .iter()
        .map(|item| bind_select_item(item, function_registry))
        .collect()
}

fn bind_select_item(
    item: &SelectItem,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    match item {
        SelectItem::UnnamedExpr(expr) => bind_expr(expr, function_registry),
        SelectItem::ExprWithAlias { expr, alias } => {
            Ok(bind_expr(expr, function_registry)?.alias(alias.value.clone()))
        }
        SelectItem::ExprWithAliases { expr, aliases } => {
            let Some(alias) = aliases.first() else {
                return Err(PlannerError::InvalidPlan {
                    reason: "projection aliases must not be empty".to_string(),
                });
            };
            Ok(bind_expr(expr, function_registry)?.alias(alias.value.clone()))
        }
        SelectItem::Wildcard(_) => Ok(wildcard_expr()),
        SelectItem::QualifiedWildcard(kind, _) => bind_qualified_wildcard(kind),
    }
}

#[allow(deprecated)]
fn bind_qualified_wildcard(
    kind: &SelectItemQualifiedWildcardKind,
) -> Result<DataFusionExpr, PlannerError> {
    match kind {
        SelectItemQualifiedWildcardKind::ObjectName(name) => Ok(DataFusionExpr::Wildcard {
            qualifier: Some(name.to_string().into()),
            options: Box::new(WildcardOptions::default()),
        }),
        SelectItemQualifiedWildcardKind::Expr(expr) => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported qualified wildcard expression `{expr}`"),
        }),
    }
}

fn build_table_with_joins(
    from: &TableWithJoins,
    tables: &[brewdb_catalog::TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_table_factor(&from.relation, tables)?;
    for join in &from.joins {
        input = build_join(input, join, tables, function_registry)?;
    }
    Ok(input)
}

fn build_join(
    left: DataFusionLogicalPlan,
    join: &Join,
    tables: &[brewdb_catalog::TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let right = build_table_factor(&join.relation, tables)?;
    if matches!(join.join_operator, AstJoinOperator::CrossJoin(_)) {
        return LogicalPlanBuilder::from(left)
            .cross_join(right)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error);
    }
    let (join_type, condition) = bind_join_operator(&join.join_operator, function_registry)?;
    let condition = condition.unwrap_or(JoinCondition {
        left_keys: Vec::new(),
        right_keys: Vec::new(),
        filter: None,
    });
    LogicalPlanBuilder::from(left)
        .join(
            right,
            join_type,
            (condition.left_keys, condition.right_keys),
            condition.filter,
        )
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)
}

fn bind_join_operator(
    join_operator: &AstJoinOperator,
    function_registry: &dyn FunctionRegistry,
) -> Result<(DataFusionJoinType, Option<JoinCondition>), PlannerError> {
    match join_operator {
        AstJoinOperator::Join(constraint) | AstJoinOperator::Inner(constraint) => Ok((
            DataFusionJoinType::Inner,
            bind_join_constraint(constraint, function_registry)?,
        )),
        AstJoinOperator::Left(constraint) | AstJoinOperator::LeftOuter(constraint) => Ok((
            DataFusionJoinType::Left,
            bind_join_constraint(constraint, function_registry)?,
        )),
        AstJoinOperator::Right(constraint) | AstJoinOperator::RightOuter(constraint) => Ok((
            DataFusionJoinType::Right,
            bind_join_constraint(constraint, function_registry)?,
        )),
        AstJoinOperator::FullOuter(constraint) => Ok((
            DataFusionJoinType::Full,
            bind_join_constraint(constraint, function_registry)?,
        )),
        AstJoinOperator::CrossJoin(_) => unreachable!("cross join handled before join binding"),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported join operator `{:?}`", other),
        }),
    }
}

fn bind_join_constraint(
    constraint: &JoinConstraint,
    function_registry: &dyn FunctionRegistry,
) -> Result<Option<JoinCondition>, PlannerError> {
    match constraint {
        JoinConstraint::On(expr) => {
            let condition = bind_expr(expr, function_registry)?;
            let structured = extract_join_condition(condition);
            Ok(
                (!structured.left_keys.is_empty() || structured.filter.is_some())
                    .then_some(structured),
            )
        }
        JoinConstraint::None => Ok(None),
        JoinConstraint::Natural => Err(PlannerError::UnsupportedPlan {
            reason: "natural join is not supported yet".to_string(),
        }),
        JoinConstraint::Using(columns) => Err(PlannerError::UnsupportedPlan {
            reason: format!("join using is not supported yet: {:?}", columns),
        }),
    }
}

fn extract_join_condition(condition: DataFusionExpr) -> JoinCondition {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut filters = Vec::new();
    collect_join_predicates(condition, &mut left_keys, &mut right_keys, &mut filters);
    JoinCondition {
        left_keys,
        right_keys,
        filter: combine_conjuncts(filters),
    }
}

fn collect_join_predicates(
    expr: DataFusionExpr,
    left_keys: &mut Vec<Column>,
    right_keys: &mut Vec<Column>,
    filters: &mut Vec<DataFusionExpr>,
) {
    match expr {
        DataFusionExpr::BinaryExpr(binary) if binary.op == DataFusionOperator::And => {
            collect_join_predicates(*binary.left, left_keys, right_keys, filters);
            collect_join_predicates(*binary.right, left_keys, right_keys, filters);
        }
        DataFusionExpr::BinaryExpr(binary) if binary.op == DataFusionOperator::Eq => {
            match (
                extract_column(binary.left.as_ref()),
                extract_column(binary.right.as_ref()),
            ) {
                (Some(left), Some(right)) => {
                    left_keys.push(left);
                    right_keys.push(right);
                }
                _ => filters.push(DataFusionExpr::BinaryExpr(binary)),
            }
        }
        other => filters.push(other),
    }
}

fn extract_column(expr: &DataFusionExpr) -> Option<Column> {
    match expr {
        DataFusionExpr::Column(column) => Some(column.clone()),
        _ => None,
    }
}

fn combine_conjuncts(filters: Vec<DataFusionExpr>) -> Option<DataFusionExpr> {
    filters.into_iter().reduce(|left, right| {
        DataFusionExpr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            DataFusionOperator::And,
            Box::new(right),
        ))
    })
}

fn build_table_factor(
    factor: &TableFactor,
    tables: &[brewdb_catalog::TableCatalogEntry],
) -> Result<DataFusionLogicalPlan, PlannerError> {
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions,
            json_path,
            sample,
            index_hints,
            with_ordinality,
        } => {
            if args.is_some()
                || !with_hints.is_empty()
                || version.is_some()
                || !partitions.is_empty()
                || json_path.is_some()
                || sample.is_some()
                || !index_hints.is_empty()
                || *with_ordinality
            {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            }
            let table = resolve_table_entry(name, tables)?;
            let scan_name = alias_name(alias).unwrap_or_else(|| name.to_string());
            let table_source: Arc<dyn TableSource> = Arc::new(table.clone());
            LogicalPlanBuilder::scan(scan_name, table_source, None)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported table factor `{other}`"),
        }),
    }
}

fn resolve_table_entry(
    name: &datafusion_sql::sqlparser::ast::ObjectName,
    tables: &[TableCatalogEntry],
) -> Result<TableCatalogEntry, PlannerError> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.as_str())
                .ok_or_else(|| PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table name part `{part}`"),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let matches_path = |table: &TableCatalogEntry| match parts.as_slice() {
        [table_name] => table.path.table() == *table_name,
        [database_name, table_name] => {
            table.path.database() == *database_name && table.path.table() == *table_name
        }
        [catalog_name, database_name, table_name] => {
            table.path.catalog() == *catalog_name
                && table.path.database() == *database_name
                && table.path.table() == *table_name
        }
        _ => false,
    };
    let mut matches = tables.iter().filter(|table| matches_path(table));
    let Some(table) = matches.next() else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("bound table `{name}` not found"),
        });
    };
    if matches.next().is_some() {
        return Err(PlannerError::InvalidPlan {
            reason: format!("bound table `{name}` is ambiguous"),
        });
    }
    Ok(table.clone())
}

fn alias_name(alias: &Option<TableAlias>) -> Option<String> {
    alias.as_ref().map(|alias| alias.name.value.clone())
}

#[allow(deprecated)]
fn projection_is_passthrough_wildcard(projection: &[DataFusionExpr]) -> bool {
    matches!(
        projection,
        [DataFusionExpr::Wildcard {
            qualifier: None,
            ..
        }]
    )
}

#[allow(deprecated)]
fn wildcard_expr() -> DataFusionExpr {
    DataFusionExpr::Wildcard {
        qualifier: None,
        options: Box::default(),
    }
}

fn map_df_plan_error(error: datafusion_common::DataFusionError) -> PlannerError {
    PlannerError::InvalidPlan {
        reason: error.to_string(),
    }
}

fn bind_group_by(
    group_by: &GroupByExpr,
    function_registry: &dyn FunctionRegistry,
) -> Result<QueryGroupBy, PlannerError> {
    match group_by {
        GroupByExpr::All(_) => Ok(QueryGroupBy::All),
        GroupByExpr::Expressions(expressions, _) if expressions.is_empty() => {
            Ok(QueryGroupBy::None)
        }
        GroupByExpr::Expressions(expressions, _) => Ok(QueryGroupBy::Expressions(
            expressions
                .iter()
                .map(|expr| bind_expr(expr, function_registry))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

fn bind_expr(
    expr: &AstExpr,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        AstExpr::Identifier(ident) => Ok(col(ident.to_string())),
        AstExpr::CompoundIdentifier(idents) => Ok(col(idents
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("."))),
        AstExpr::Value(value) => bind_value(value),
        AstExpr::Nested(expr) => bind_expr(expr, function_registry),
        AstExpr::BinaryOp { left, op, right } => Ok(DataFusionExpr::BinaryExpr(BinaryExpr::new(
            Box::new(bind_expr(left, function_registry)?),
            bind_binary_operator(op)?,
            Box::new(bind_expr(right, function_registry)?),
        ))),
        AstExpr::UnaryOp { op, expr } => bind_unary_expr(op, expr, function_registry),
        AstExpr::IsNull(expr) => Ok(DataFusionExpr::IsNull(Box::new(bind_expr(
            expr,
            function_registry,
        )?))),
        AstExpr::IsNotNull(expr) => Ok(DataFusionExpr::IsNotNull(Box::new(bind_expr(
            expr,
            function_registry,
        )?))),
        AstExpr::Function(function) => bind_function(function, function_registry),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported expression `{other}`"),
        }),
    }
}

fn bind_unary_expr(
    op: &AstUnaryOperator,
    expr: &AstExpr,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    let expr = bind_expr(expr, function_registry)?;
    match op {
        AstUnaryOperator::Not => Ok(DataFusionExpr::Not(Box::new(expr))),
        AstUnaryOperator::Minus => Ok(DataFusionExpr::Negative(Box::new(expr))),
        AstUnaryOperator::Plus => Ok(expr),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported unary operator `{other}`"),
        }),
    }
}

fn bind_binary_operator(op: &AstBinaryOperator) -> Result<DataFusionOperator, PlannerError> {
    match op {
        AstBinaryOperator::Plus => Ok(DataFusionOperator::Plus),
        AstBinaryOperator::Minus => Ok(DataFusionOperator::Minus),
        AstBinaryOperator::Multiply => Ok(DataFusionOperator::Multiply),
        AstBinaryOperator::Divide => Ok(DataFusionOperator::Divide),
        AstBinaryOperator::Modulo => Ok(DataFusionOperator::Modulo),
        AstBinaryOperator::StringConcat => Ok(DataFusionOperator::StringConcat),
        AstBinaryOperator::Gt => Ok(DataFusionOperator::Gt),
        AstBinaryOperator::Lt => Ok(DataFusionOperator::Lt),
        AstBinaryOperator::GtEq => Ok(DataFusionOperator::GtEq),
        AstBinaryOperator::LtEq => Ok(DataFusionOperator::LtEq),
        AstBinaryOperator::Eq => Ok(DataFusionOperator::Eq),
        AstBinaryOperator::NotEq => Ok(DataFusionOperator::NotEq),
        AstBinaryOperator::And => Ok(DataFusionOperator::And),
        AstBinaryOperator::Or => Ok(DataFusionOperator::Or),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported binary operator `{other}`"),
        }),
    }
}

fn bind_value(
    value: &datafusion_sql::sqlparser::ast::ValueWithSpan,
) -> Result<DataFusionExpr, PlannerError> {
    match &value.value {
        Value::Number(number, _) => {
            if let Ok(parsed) = number.parse::<i64>() {
                Ok(lit(parsed))
            } else if let Ok(parsed) = number.parse::<f64>() {
                Ok(lit(parsed))
            } else {
                Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported numeric literal `{number}`"),
                })
            }
        }
        Value::SingleQuotedString(inner)
        | Value::DoubleQuotedString(inner)
        | Value::EscapedStringLiteral(inner)
        | Value::UnicodeStringLiteral(inner)
        | Value::NationalStringLiteral(inner)
        | Value::HexStringLiteral(inner)
        | Value::SingleQuotedByteStringLiteral(inner)
        | Value::DoubleQuotedByteStringLiteral(inner)
        | Value::TripleSingleQuotedString(inner)
        | Value::TripleDoubleQuotedString(inner)
        | Value::TripleSingleQuotedByteStringLiteral(inner)
        | Value::TripleDoubleQuotedByteStringLiteral(inner)
        | Value::SingleQuotedRawStringLiteral(inner)
        | Value::DoubleQuotedRawStringLiteral(inner)
        | Value::TripleSingleQuotedRawStringLiteral(inner)
        | Value::TripleDoubleQuotedRawStringLiteral(inner) => Ok(lit(inner.clone())),
        Value::Boolean(value) => Ok(lit(*value)),
        Value::Null => Ok(lit(ScalarValue::Null)),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported literal `{other}`"),
        }),
    }
}

fn bind_function(
    function: &datafusion_sql::sqlparser::ast::Function,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    if function.over.is_some()
        || !function.within_group.is_empty()
        || function.parameters != FunctionArguments::None
    {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported function shape `{function}`"),
        });
    }
    let args = match &function.args {
        FunctionArguments::List(arguments) => arguments
            .args
            .iter()
            .map(|arg| bind_function_arg(arg, function_registry))
            .collect::<Result<Vec<_>, _>>()?,
        FunctionArguments::None => Vec::new(),
        FunctionArguments::Subquery(query) => {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported function subquery argument `{query}`"),
            });
        }
    };
    let function_name = function.name.to_string();
    if let Ok(udf) = function_registry.udf(&function_name) {
        if function.filter.is_some() || function.null_treatment.is_some() {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported scalar function shape `{function}`"),
            });
        }
        return Ok(DataFusionExpr::ScalarFunction(ScalarFunction::new_udf(
            udf, args,
        )));
    }
    if let Ok(udaf) = function_registry.udaf(&function_name) {
        let distinct = match &function.args {
            FunctionArguments::List(arguments) => {
                matches!(
                    arguments.duplicate_treatment,
                    Some(DuplicateTreatment::Distinct)
                )
            }
            FunctionArguments::None | FunctionArguments::Subquery(_) => false,
        };
        let filter = function
            .filter
            .as_ref()
            .map(|expr| bind_expr(expr, function_registry))
            .transpose()?
            .map(Box::new);
        if function.null_treatment.is_some() {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported aggregate function null treatment `{function}`"),
            });
        }
        return Ok(DataFusionExpr::AggregateFunction(
            AggregateFunction::new_udf(udaf, args, distinct, filter, Vec::new(), None),
        ));
    }
    Err(PlannerError::UnsupportedPlan {
        reason: format!("function `{function_name}` not found in DataFusion function registry"),
    })
}

fn bind_function_arg(
    arg: &FunctionArg,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => bind_expr(expr, function_registry),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(wildcard_expr()),
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => {
            bind_qualified_wildcard(&SelectItemQualifiedWildcardKind::ObjectName(name.clone()))
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported function argument `{other}`"),
        }),
    }
}
