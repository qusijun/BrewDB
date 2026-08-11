use std::sync::Arc;

use crate::errors::PlannerError;
use brewdb_catalog::TableCatalogEntry;
use brewdb_sql_parser::ast::{
    Join, JoinConstraint, JoinOperator as AstJoinOperator, Query, Select, SetExpr,
    Statement as AstStatement, TableAlias, TableFactor, TableWithJoins,
};
use datafusion_common::Column;
use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    BinaryExpr, Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
    Operator as DataFusionOperator, TableSource,
};

use crate::errors::map_df_plan_error;
use crate::logical::expr::{
    QueryGroupBy, bind_expr, bind_group_by, bind_projection, projection_is_passthrough_wildcard,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueryExpression {
    distinct: bool,
    projection: Vec<DataFusionExpr>,
    selection: Option<DataFusionExpr>,
    group_by: QueryGroupBy,
    having: Option<DataFusionExpr>,
}

#[derive(Debug)]
struct JoinCondition {
    left_keys: Vec<Column>,
    right_keys: Vec<Column>,
    filter: Option<DataFusionExpr>,
}

pub(crate) fn plan_query_statement(
    ast: AstStatement,
    tables: Vec<TableCatalogEntry>,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let query = bind_query_expression(&ast, function_registry)?;
    build_query_input(&ast, &tables, &query, function_registry)
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
    name: &brewdb_sql_parser::ast::ObjectName,
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
