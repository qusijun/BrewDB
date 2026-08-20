use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::TableCatalogEntry;
use crate::parser::ast::{
    Distinct as AstDistinct, Join, JoinConstraint, JoinOperator as AstJoinOperator, LimitClause,
    OrderBy, OrderByKind, Query, Select, SetExpr, Statement as AstStatement, TableAlias,
    TableFactor, TableWithJoins,
};
use crate::planner::PlannerError;
use arrow::datatypes::FieldRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::expr::Sort as DataFusionSort;
use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::expr_as_column_expr;
use datafusion_expr::{
    BinaryExpr, Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
    Operator as DataFusionOperator, TableSource,
};

use crate::planner::errors::{map_common_error, map_df_plan_error};
use crate::planner::logical::expr::{
    bind_expr_with_subqueries, bind_group_by_with_subqueries, bind_projection_with_subqueries,
    projection_is_passthrough_wildcard, QueryGroupBy,
};
use crate::planner::logical::table_source::DefaultTableSource;
use crate::planner::logical::{
    resolve_query_tables, LogicalPlanningContext, LogicalPlanningSession,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryExpression {
    distinct: bool,
    projection: Vec<DataFusionExpr>,
    selection: Option<DataFusionExpr>,
    group_by: QueryGroupBy,
    having: Option<DataFusionExpr>,
    order_by: Vec<DataFusionSort>,
    limit: Option<QueryLimit>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryLimit {
    skip: Option<DataFusionExpr>,
    fetch: Option<DataFusionExpr>,
}

#[derive(Debug)]
struct JoinCondition {
    left_keys: Vec<Column>,
    right_keys: Vec<Column>,
    filter: Option<DataFusionExpr>,
}

#[derive(Clone, Debug, Default)]
struct QueryBindScope {
    local: Vec<VisibleField>,
    outer: Vec<VisibleField>,
}

#[derive(Clone, Debug)]
struct VisibleField {
    qualifier: Option<String>,
    name: String,
    field: FieldRef,
}

pub(crate) fn plan_query_statement(
    ast: AstStatement,
    tables: Vec<TableCatalogEntry>,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    plan_query_statement_with_outer(ast, tables, function_registry, Vec::new())
}

fn plan_query_statement_with_outer(
    ast: AstStatement,
    tables: Vec<TableCatalogEntry>,
    function_registry: &dyn FunctionRegistry,
    outer_scope: Vec<VisibleField>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let query = bind_query_expression(&ast, &tables, function_registry, outer_scope)?;
    build_query_input(&ast, &tables, &query, function_registry)
}

pub(crate) fn bind_query_statement(
    ast: AstStatement,
    session: &LogicalPlanningSession,
    ctx: &LogicalPlanningContext<'_>,
    query: &Query,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let tables = resolve_query_tables(ctx, session, query)?;
    let plan = plan_query_statement(ast, tables.clone(), function_registry)?;
    let _ = tables;
    Ok(plan)
}

fn bind_query_expression(
    statement: &AstStatement,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    outer_scope: Vec<VisibleField>,
) -> Result<QueryExpression, PlannerError> {
    let AstStatement::Query(query) = statement else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{statement}`"),
        });
    };
    bind_select_query(query, tables, function_registry, outer_scope)
}

fn build_query_input(
    ast: &AstStatement,
    tables: &[TableCatalogEntry],
    query: &QueryExpression,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_from_input(ast, tables, function_registry)?;
    if let Some(predicate) = &query.selection {
        input = LogicalPlanBuilder::from(input)
            .filter(predicate.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if needs_aggregate(query) {
        let aggregates = aggregate_exprs(query);
        let group_keys = match &query.group_by {
            QueryGroupBy::Expressions(expressions) => {
                resolve_group_by_positions(expressions, &query.projection)?
            }
            QueryGroupBy::None | QueryGroupBy::All => Vec::new(),
        };
        input = LogicalPlanBuilder::from(input)
            .aggregate(group_keys, aggregates)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
        let aggregate_projection_exprs = aggregate_projection_exprs(&input)?;
        if let Some(predicate) = &query.having {
            let predicate = rebase_expr(predicate, &aggregate_projection_exprs, &input)?;
            input = LogicalPlanBuilder::from(input)
                .filter(predicate)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)?;
        }
        let projection = query
            .projection
            .iter()
            .map(|expr| rebase_expr(expr, &aggregate_projection_exprs, &input))
            .collect::<Result<Vec<_>, _>>()?;
        input = LogicalPlanBuilder::from(input)
            .project(projection)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    } else if !projection_is_passthrough_wildcard(&query.projection) {
        input = LogicalPlanBuilder::from(input)
            .project(query.projection.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if query.distinct {
        input = LogicalPlanBuilder::from(input)
            .distinct()
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if !query.order_by.is_empty() {
        let order_by = resolve_sort_positions(&query.order_by, &query.projection)?;
        input = LogicalPlanBuilder::from(input)
            .sort(order_by)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    if let Some(limit) = &query.limit {
        input = LogicalPlanBuilder::from(input)
            .limit_by_expr(limit.skip.clone(), limit.fetch.clone())
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error)?;
    }
    Ok(input)
}

fn build_from_input(
    ast: &AstStatement,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let AstStatement::Query(query) = ast else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected query statement, got `{ast}`"),
        });
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{}`", query.body),
        });
    };
    if select.from.is_empty() {
        return LogicalPlanBuilder::empty(true)
            .build()
            .map_err(map_df_plan_error);
    }
    let mut inputs = select
        .from
        .iter()
        .map(|from| build_table_with_joins(from, tables, function_registry))
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
            .chain(query.having.iter())
            .any(expr_contains_aggregate),
    }
}

fn aggregate_exprs(query: &QueryExpression) -> Vec<DataFusionExpr> {
    let mut aggregates = Vec::new();
    for expr in query.projection.iter().chain(query.having.iter()) {
        collect_aggregate_exprs(expr, &mut aggregates);
    }
    aggregates
}

fn collect_aggregate_exprs(expr: &DataFusionExpr, aggregates: &mut Vec<DataFusionExpr>) {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    let _ = expr.apply(|node| {
        if matches!(
            node,
            DataFusionExpr::ScalarSubquery(_)
                | DataFusionExpr::InSubquery(_)
                | DataFusionExpr::Exists(_)
        ) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if matches!(node, DataFusionExpr::AggregateFunction(_)) && !aggregates.contains(node) {
            aggregates.push(node.clone());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

fn expr_contains_aggregate(expr: &DataFusionExpr) -> bool {
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};

    let mut found = false;
    let _ = expr.apply(|node| {
        if matches!(
            node,
            DataFusionExpr::ScalarSubquery(_)
                | DataFusionExpr::InSubquery(_)
                | DataFusionExpr::Exists(_)
        ) {
            return Ok(TreeNodeRecursion::Jump);
        }
        if matches!(node, DataFusionExpr::AggregateFunction(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn aggregate_projection_exprs(
    plan: &DataFusionLogicalPlan,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    let DataFusionLogicalPlan::Aggregate(aggregate) = plan else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("expected aggregate plan, got `{plan:?}`"),
        });
    };
    Ok(aggregate
        .group_expr
        .iter()
        .chain(aggregate.aggr_expr.iter())
        .cloned()
        .collect())
}

fn rebase_expr(
    expr: &DataFusionExpr,
    base_exprs: &[DataFusionExpr],
    plan: &DataFusionLogicalPlan,
) -> Result<DataFusionExpr, PlannerError> {
    use datafusion_common::tree_node::TreeNodeRecursion;

    expr.clone()
        .transform_down(|nested_expr| {
            if matches!(
                nested_expr,
                DataFusionExpr::ScalarSubquery(_)
                    | DataFusionExpr::InSubquery(_)
                    | DataFusionExpr::Exists(_)
            ) {
                return Ok(Transformed::new(
                    nested_expr,
                    false,
                    TreeNodeRecursion::Jump,
                ));
            }
            if let Some(base_expr) = matching_base_expr(&nested_expr, base_exprs) {
                expr_as_column_expr(base_expr, plan).map(Transformed::yes)
            } else {
                Ok(Transformed::no(nested_expr))
            }
        })
        .data()
        .map_err(map_df_plan_error)
}

fn matching_base_expr<'a>(
    expr: &DataFusionExpr,
    base_exprs: &'a [DataFusionExpr],
) -> Option<&'a DataFusionExpr> {
    base_exprs
        .iter()
        .find(|base_expr| *base_expr == expr)
        .or_else(|| {
            base_exprs.iter().find(|base_expr| {
                matches!(base_expr, DataFusionExpr::AggregateFunction(_))
                    && matches!(expr, DataFusionExpr::AggregateFunction(_))
                    && normalize_expr_name(&base_expr.schema_name().to_string())
                        == normalize_expr_name(&expr.schema_name().to_string())
            })
        })
}

fn normalize_expr_name(name: &str) -> String {
    let mut normalized = String::new();
    let mut token_start = 0;
    for ch in name.chars() {
        if ch == '.' {
            normalized.truncate(token_start);
            token_start = normalized.len();
            continue;
        }
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            token_start = normalized.len() + ch.len_utf8();
        }
        normalized.push(ch);
    }
    normalized
}

fn bind_select_query(
    query: &Query,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    outer_scope: Vec<VisibleField>,
) -> Result<QueryExpression, PlannerError> {
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            bind_select(query, select, tables, function_registry, outer_scope)
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported query body `{other}`"),
        }),
    }
}

fn bind_select(
    query: &Query,
    select: &Select,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    outer_scope: Vec<VisibleField>,
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
    let scope = QueryBindScope {
        local: visible_fields_for_select(select, tables)?,
        outer: outer_scope,
    };
    let projection =
        bind_projection_for_query(&select.projection, tables, function_registry, &scope)?;
    let aliases = extract_aliases(&projection);
    let group_by =
        match bind_group_by_for_query(&select.group_by, tables, function_registry, &scope)? {
            QueryGroupBy::Expressions(expressions) => QueryGroupBy::Expressions(
                expressions
                    .into_iter()
                    .map(|expr| resolve_aliases_to_exprs(expr, &aliases))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            group_by => group_by,
        };
    let having = select
        .having
        .as_ref()
        .map(|expr| bind_expr_for_query(expr, tables, function_registry, &scope))
        .transpose()?
        .map(|expr| resolve_aliases_to_exprs(expr, &aliases))
        .transpose()?;
    Ok(QueryExpression {
        distinct: bind_distinct(&select.distinct)?,
        order_by: bind_order_by(query.order_by.as_ref(), tables, function_registry, &scope)?,
        limit: bind_limit(
            query.limit_clause.as_ref(),
            tables,
            function_registry,
            &scope,
        )?,
        projection,
        selection: select
            .selection
            .as_ref()
            .map(|expr| bind_expr_for_query(expr, tables, function_registry, &scope))
            .transpose()?,
        group_by,
        having,
    })
}

fn bind_expr_for_query(
    expr: &crate::parser::ast::Expr,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    scope: &QueryBindScope,
) -> Result<DataFusionExpr, PlannerError> {
    let child_outer_scope = scope.child_outer_scope();
    let mut subquery_planner = |subquery: &Query| {
        plan_query_statement_with_outer(
            AstStatement::Query(Box::new(subquery.clone())),
            tables.to_vec(),
            function_registry,
            child_outer_scope.clone(),
        )
    };
    let expr = bind_expr_with_subqueries(expr, function_registry, &mut subquery_planner)?;
    rewrite_outer_references(expr, scope)
}

fn bind_projection_for_query(
    projection: &[crate::parser::ast::SelectItem],
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    let child_outer_scope = scope.child_outer_scope();
    let mut subquery_planner = |subquery: &Query| {
        plan_query_statement_with_outer(
            AstStatement::Query(Box::new(subquery.clone())),
            tables.to_vec(),
            function_registry,
            child_outer_scope.clone(),
        )
    };
    bind_projection_with_subqueries(projection, function_registry, &mut subquery_planner)?
        .into_iter()
        .map(|expr| rewrite_outer_references(expr, scope))
        .collect()
}

fn bind_group_by_for_query(
    group_by: &crate::parser::ast::GroupByExpr,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    scope: &QueryBindScope,
) -> Result<QueryGroupBy, PlannerError> {
    let child_outer_scope = scope.child_outer_scope();
    let mut subquery_planner = |subquery: &Query| {
        plan_query_statement_with_outer(
            AstStatement::Query(Box::new(subquery.clone())),
            tables.to_vec(),
            function_registry,
            child_outer_scope.clone(),
        )
    };
    match bind_group_by_with_subqueries(group_by, function_registry, &mut subquery_planner)? {
        QueryGroupBy::Expressions(expressions) => Ok(QueryGroupBy::Expressions(
            expressions
                .into_iter()
                .map(|expr| rewrite_outer_references(expr, scope))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        group_by => Ok(group_by),
    }
}

impl QueryBindScope {
    fn child_outer_scope(&self) -> Vec<VisibleField> {
        self.local
            .iter()
            .cloned()
            .chain(self.outer.iter().cloned())
            .collect()
    }
}

fn rewrite_outer_references(
    expr: DataFusionExpr,
    scope: &QueryBindScope,
) -> Result<DataFusionExpr, PlannerError> {
    expr.transform_up(|nested_expr| match nested_expr {
        DataFusionExpr::Column(column) => {
            if scope.matches_local(&column) {
                return Ok(Transformed::no(DataFusionExpr::Column(column)));
            }
            if let Some(field) = scope.resolve_outer(&column) {
                return Ok(Transformed::yes(DataFusionExpr::OuterReferenceColumn(
                    field.field.clone(),
                    Column::new(column.relation.clone(), column.name.clone()),
                )));
            }
            Ok(Transformed::no(DataFusionExpr::Column(column)))
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
    .map_err(map_df_plan_error)
}

impl QueryBindScope {
    fn matches_local(&self, column: &Column) -> bool {
        self.local.iter().any(|field| field.matches(column))
    }

    fn resolve_outer(&self, column: &Column) -> Option<&VisibleField> {
        let mut matches = self.outer.iter().filter(|field| field.matches(column));
        let field = matches.next()?;
        matches.next().is_none().then_some(field)
    }
}

impl VisibleField {
    fn matches(&self, column: &Column) -> bool {
        if self.name != column.name {
            return false;
        }
        let Some(relation) = &column.relation else {
            return true;
        };
        self.qualifier.as_deref().is_some_and(|qualifier| {
            qualifier == relation.table() || qualifier == relation.to_string()
        })
    }
}

fn visible_fields_for_select(
    select: &Select,
    tables: &[TableCatalogEntry],
) -> Result<Vec<VisibleField>, PlannerError> {
    let mut fields = Vec::new();
    for from in &select.from {
        collect_visible_fields_for_table_with_joins(from, tables, &mut fields)?;
    }
    Ok(fields)
}

fn collect_visible_fields_for_table_with_joins(
    from: &TableWithJoins,
    tables: &[TableCatalogEntry],
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    collect_visible_fields_for_table_factor(&from.relation, tables, fields)?;
    for join in &from.joins {
        collect_visible_fields_for_table_factor(&join.relation, tables, fields)?;
    }
    Ok(())
}

fn collect_visible_fields_for_table_factor(
    factor: &TableFactor,
    tables: &[TableCatalogEntry],
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table = resolve_table_entry(name, tables)?;
            let qualifier = alias_name(alias).unwrap_or_else(|| name.to_string());
            append_table_visible_fields(&table, Some(qualifier), fields)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_visible_fields_for_table_with_joins(table_with_joins, tables, fields),
        TableFactor::Derived { alias, .. } => {
            if let Some(alias) = alias {
                for column in &alias.columns {
                    fields.push(VisibleField {
                        qualifier: Some(alias.name.value.clone()),
                        name: column.name.value.clone(),
                        field: Arc::new(arrow::datatypes::Field::new(
                            column.name.value.clone(),
                            arrow::datatypes::DataType::Null,
                            true,
                        )),
                    });
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn append_table_visible_fields(
    table: &TableCatalogEntry,
    qualifier: Option<String>,
    fields: &mut Vec<VisibleField>,
) -> Result<(), PlannerError> {
    for field in &table.table_schema.fields {
        fields.push(VisibleField {
            qualifier: qualifier.clone(),
            name: field.name.clone(),
            field: Arc::new(field.to_arrow_field().map_err(map_common_error)?),
        });
    }
    Ok(())
}

fn visible_fields_for_tables(
    tables: &[TableCatalogEntry],
) -> Result<Vec<VisibleField>, PlannerError> {
    let mut fields = Vec::new();
    for table in tables {
        append_table_visible_fields(table, Some(table.path.table().to_owned()), &mut fields)?;
    }
    Ok(fields)
}

fn bind_distinct(distinct: &Option<AstDistinct>) -> Result<bool, PlannerError> {
    match distinct {
        None | Some(AstDistinct::All) => Ok(false),
        Some(AstDistinct::Distinct) => Ok(true),
        Some(AstDistinct::On(exprs)) => Err(PlannerError::UnsupportedPlan {
            reason: format!("distinct on is not supported yet: {exprs:?}"),
        }),
    }
}

fn bind_order_by(
    order_by: Option<&OrderBy>,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    scope: &QueryBindScope,
) -> Result<Vec<DataFusionSort>, PlannerError> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    if order_by.interpolate.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported order by shape `{order_by}`"),
        });
    }
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported order by shape `{order_by}`"),
        });
    };
    expressions
        .iter()
        .map(|expr| {
            if expr.with_fill.is_some() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported order by expression `{expr}`"),
                });
            }
            Ok(DataFusionSort::new(
                bind_expr_for_query(&expr.expr, tables, function_registry, scope)?,
                expr.options.asc.unwrap_or(true),
                expr.options.nulls_first.unwrap_or(false),
            ))
        })
        .collect()
}

fn bind_limit(
    limit_clause: Option<&LimitClause>,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
    scope: &QueryBindScope,
) -> Result<Option<QueryLimit>, PlannerError> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    let limit = match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("limit by is not supported yet: {limit_by:?}"),
                });
            }
            QueryLimit {
                skip: offset
                    .as_ref()
                    .map(|offset| {
                        bind_expr_for_query(&offset.value, tables, function_registry, scope)
                    })
                    .transpose()?,
                fetch: limit
                    .as_ref()
                    .map(|limit| bind_expr_for_query(limit, tables, function_registry, scope))
                    .transpose()?,
            }
        }
        LimitClause::OffsetCommaLimit { offset, limit } => QueryLimit {
            skip: Some(bind_expr_for_query(
                offset,
                tables,
                function_registry,
                scope,
            )?),
            fetch: Some(bind_expr_for_query(
                limit,
                tables,
                function_registry,
                scope,
            )?),
        },
    };
    Ok(Some(limit))
}

fn resolve_group_by_positions(
    group_by: &[DataFusionExpr],
    projection: &[DataFusionExpr],
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    group_by
        .iter()
        .map(|expr| match expr {
            DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _)
                if *position > 0 && (*position as usize) <= projection.len() =>
            {
                Ok(unalias_expr(projection[*position as usize - 1].clone()))
            }
            DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _) => {
                Err(PlannerError::InvalidPlan {
                    reason: format!("GROUP BY position {position} is out of range"),
                })
            }
            _ => Ok(expr.clone()),
        })
        .collect()
}

fn unalias_expr(expr: DataFusionExpr) -> DataFusionExpr {
    match expr {
        DataFusionExpr::Alias(alias) => *alias.expr,
        _ => expr,
    }
}

fn resolve_sort_positions(
    sort_exprs: &[DataFusionSort],
    projection: &[DataFusionExpr],
) -> Result<Vec<DataFusionSort>, PlannerError> {
    sort_exprs
        .iter()
        .map(|sort| {
            Ok(DataFusionSort::new(
                resolve_position_to_expr(sort.expr.clone(), projection)?,
                sort.asc,
                sort.nulls_first,
            ))
        })
        .collect()
}

fn resolve_position_to_expr(
    expr: DataFusionExpr,
    projection: &[DataFusionExpr],
) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _)
            if position > 0 && (position as usize) <= projection.len() =>
        {
            Ok(projected_column_expr(&projection[position as usize - 1]))
        }
        DataFusionExpr::Literal(ScalarValue::Int64(Some(position)), _) => {
            Err(PlannerError::InvalidPlan {
                reason: format!("ORDER BY position {position} is out of range"),
            })
        }
        _ => Ok(expr),
    }
}

fn projected_column_expr(expr: &DataFusionExpr) -> DataFusionExpr {
    match expr {
        DataFusionExpr::Alias(alias) => {
            DataFusionExpr::Column(Column::from_name(alias.name.clone()))
        }
        DataFusionExpr::Column(column) => DataFusionExpr::Column(column.clone()),
        _ => DataFusionExpr::Column(Column::from_name(expr.schema_name().to_string())),
    }
}

fn extract_aliases(exprs: &[DataFusionExpr]) -> HashMap<String, DataFusionExpr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            DataFusionExpr::Alias(alias) => Some((alias.name.clone(), *alias.expr.clone())),
            _ => None,
        })
        .collect()
}

fn resolve_aliases_to_exprs(
    expr: DataFusionExpr,
    aliases: &HashMap<String, DataFusionExpr>,
) -> Result<DataFusionExpr, PlannerError> {
    expr.transform_up(|nested_expr| match nested_expr {
        DataFusionExpr::Column(column) if column.relation.is_none() => {
            if let Some(alias_expr) = aliases.get(&column.name) {
                Ok(Transformed::yes(alias_expr.clone()))
            } else {
                Ok(Transformed::no(DataFusionExpr::Column(column)))
            }
        }
        other => Ok(Transformed::no(other)),
    })
    .data()
    .map_err(map_df_plan_error)
}

fn build_table_with_joins(
    from: &TableWithJoins,
    tables: &[crate::catalog::TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_table_factor(&from.relation, tables, function_registry)?;
    for join in &from.joins {
        input = build_join(input, join, tables, function_registry)?;
    }
    Ok(input)
}

fn build_join(
    left: DataFusionLogicalPlan,
    join: &Join,
    tables: &[crate::catalog::TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let right = build_table_factor(&join.relation, tables, function_registry)?;
    if matches!(join.join_operator, AstJoinOperator::CrossJoin(_)) {
        return LogicalPlanBuilder::from(left)
            .cross_join(right)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error);
    }
    let (join_type, condition) =
        bind_join_operator(&join.join_operator, tables, function_registry)?;
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
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<(DataFusionJoinType, Option<JoinCondition>), PlannerError> {
    match join_operator {
        AstJoinOperator::Join(constraint) | AstJoinOperator::Inner(constraint) => Ok((
            DataFusionJoinType::Inner,
            bind_join_constraint(constraint, tables, function_registry)?,
        )),
        AstJoinOperator::Left(constraint) | AstJoinOperator::LeftOuter(constraint) => Ok((
            DataFusionJoinType::Left,
            bind_join_constraint(constraint, tables, function_registry)?,
        )),
        AstJoinOperator::Right(constraint) | AstJoinOperator::RightOuter(constraint) => Ok((
            DataFusionJoinType::Right,
            bind_join_constraint(constraint, tables, function_registry)?,
        )),
        AstJoinOperator::FullOuter(constraint) => Ok((
            DataFusionJoinType::Full,
            bind_join_constraint(constraint, tables, function_registry)?,
        )),
        AstJoinOperator::CrossJoin(_) => unreachable!("cross join handled before join binding"),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported join operator `{:?}`", other),
        }),
    }
}

fn bind_join_constraint(
    constraint: &JoinConstraint,
    tables: &[TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
) -> Result<Option<JoinCondition>, PlannerError> {
    match constraint {
        JoinConstraint::On(expr) => {
            let condition = bind_expr_for_query(
                expr,
                tables,
                function_registry,
                &QueryBindScope {
                    local: visible_fields_for_tables(tables)?,
                    outer: Vec::new(),
                },
            )?;
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
    tables: &[crate::catalog::TableCatalogEntry],
    function_registry: &dyn FunctionRegistry,
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
            let table_source: Arc<dyn TableSource> = Arc::new(DefaultTableSource::new(table));
            LogicalPlanBuilder::scan(scan_name, table_source, None)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral || sample.is_some() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            }
            let mut input = plan_query_statement(
                AstStatement::Query(Box::new(subquery.as_ref().clone())),
                tables.to_vec(),
                function_registry,
            )?;
            if let Some(alias) = alias {
                input = apply_derived_column_aliases(input, alias)?;
                input = LogicalPlanBuilder::from(input)
                    .alias(alias.name.value.clone())
                    .map_err(map_df_plan_error)?
                    .build()
                    .map_err(map_df_plan_error)?;
            }
            Ok(input)
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            if alias.is_some() {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported nested join alias `{factor}`"),
                });
            }
            build_table_with_joins(table_with_joins, tables, function_registry)
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported table factor `{other}`"),
        }),
    }
}

fn apply_derived_column_aliases(
    input: DataFusionLogicalPlan,
    alias: &TableAlias,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    if alias.columns.is_empty() {
        return Ok(input);
    }
    if let Some(column) = alias
        .columns
        .iter()
        .find(|column| column.data_type.is_some())
    {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("typed derived table column alias `{column}` is not supported yet"),
        });
    }
    let fields = input.schema().iter().collect::<Vec<_>>();
    if alias.columns.len() != fields.len() {
        return Err(PlannerError::InvalidPlan {
            reason: format!(
                "derived table alias `{}` defines {} columns but subquery returns {} columns",
                alias.name,
                alias.columns.len(),
                fields.len()
            ),
        });
    }
    let projection = fields
        .iter()
        .zip(alias.columns.iter())
        .map(|((qualifier, field), alias_column)| {
            DataFusionExpr::Column(Column::new(qualifier.cloned(), field.name()))
                .alias(alias_column.name.value.clone())
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(input)
        .project(projection)
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)
}

fn resolve_table_entry(
    name: &crate::parser::ast::ObjectName,
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
            reason: format!("planned table `{name}` not found"),
        });
    };
    if matches.next().is_some() {
        return Err(PlannerError::InvalidPlan {
            reason: format!("planned table `{name}` is ambiguous"),
        });
    }
    Ok(table.clone())
}

fn alias_name(alias: &Option<TableAlias>) -> Option<String> {
    alias.as_ref().map(|alias| alias.name.value.clone())
}
