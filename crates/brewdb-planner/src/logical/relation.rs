use crate::parser::ast::{
    Join, JoinConstraint, JoinOperator as AstJoinOperator, TableAlias, TableFactor, TableWithJoins,
};
use crate::planner::errors::map_df_plan_error;
use crate::planner::logical::context::QueryPlannerContext;
use crate::planner::logical::query::{
    bind_expr_for_query, plan_query, visible_fields_for_tables, QueryBindScope,
};
use crate::planner::PlannerError;
use datafusion_common::Column;
use datafusion_expr::logical_plan::JoinType as DataFusionJoinType;
use datafusion_expr::{
    BinaryExpr, Expr as DataFusionExpr, LogicalPlan as DataFusionLogicalPlan, LogicalPlanBuilder,
    Operator as DataFusionOperator,
};

#[derive(Debug)]
struct JoinCondition {
    left_keys: Vec<Column>,
    right_keys: Vec<Column>,
    filter: Option<DataFusionExpr>,
}

pub(super) fn build_table_with_joins(
    from: &TableWithJoins,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let mut input = build_table_factor(&from.relation, planner_context)?;
    for join in &from.joins {
        input = build_join(input, join, planner_context)?;
    }
    Ok(input)
}

fn build_join(
    left: DataFusionLogicalPlan,
    join: &Join,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let right = build_table_factor(&join.relation, planner_context)?;
    if matches!(join.join_operator, AstJoinOperator::CrossJoin(_)) {
        return LogicalPlanBuilder::from(left)
            .cross_join(right)
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error);
    }
    let (join_type, constraint) = bind_join_operator(&join.join_operator)?;
    build_join_with_constraint(left, right, join_type, constraint, planner_context)
}

fn bind_join_operator(
    join_operator: &AstJoinOperator,
) -> Result<(DataFusionJoinType, &JoinConstraint), PlannerError> {
    match join_operator {
        AstJoinOperator::Join(constraint) | AstJoinOperator::Inner(constraint) => {
            Ok((DataFusionJoinType::Inner, constraint))
        }
        AstJoinOperator::Left(constraint) | AstJoinOperator::LeftOuter(constraint) => {
            Ok((DataFusionJoinType::Left, constraint))
        }
        AstJoinOperator::Right(constraint) | AstJoinOperator::RightOuter(constraint) => {
            Ok((DataFusionJoinType::Right, constraint))
        }
        AstJoinOperator::FullOuter(constraint) => Ok((DataFusionJoinType::Full, constraint)),
        AstJoinOperator::Semi(constraint) | AstJoinOperator::LeftSemi(constraint) => {
            Ok((DataFusionJoinType::LeftSemi, constraint))
        }
        AstJoinOperator::RightSemi(constraint) => Ok((DataFusionJoinType::RightSemi, constraint)),
        AstJoinOperator::Anti(constraint) | AstJoinOperator::LeftAnti(constraint) => {
            Ok((DataFusionJoinType::LeftAnti, constraint))
        }
        AstJoinOperator::RightAnti(constraint) => Ok((DataFusionJoinType::RightAnti, constraint)),
        AstJoinOperator::CrossJoin(_) => unreachable!("cross join handled before join binding"),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported join operator `{:?}`", other),
        }),
    }
}

fn build_join_with_constraint(
    left: DataFusionLogicalPlan,
    right: DataFusionLogicalPlan,
    join_type: DataFusionJoinType,
    constraint: &JoinConstraint,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    match constraint {
        JoinConstraint::On(expr) => {
            let condition = bind_expr_for_query(
                expr,
                planner_context.tables(),
                planner_context,
                &QueryBindScope {
                    local: visible_fields_for_tables(planner_context.tables())?,
                    outer: Vec::new(),
                },
            )?;
            let structured = extract_join_condition(condition);
            LogicalPlanBuilder::from(left)
                .join(
                    right,
                    join_type,
                    (structured.left_keys, structured.right_keys),
                    structured.filter,
                )
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        JoinConstraint::Using(columns) => {
            let keys = columns
                .iter()
                .map(join_using_key)
                .collect::<Result<Vec<_>, _>>()?;
            LogicalPlanBuilder::from(left)
                .join_using(right, join_type, keys)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        JoinConstraint::Natural => {
            let left_cols: HashSet<&String> = left
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .collect();
            let keys = right
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .filter(|name| left_cols.contains(name))
                .map(Column::from_name)
                .collect::<Vec<_>>();
            if keys.is_empty() {
                return LogicalPlanBuilder::from(left)
                    .cross_join(right)
                    .map_err(map_df_plan_error)?
                    .build()
                    .map_err(map_df_plan_error);
            }
            LogicalPlanBuilder::from(left)
                .join_using(right, join_type, keys)
                .map_err(map_df_plan_error)?
                .build()
                .map_err(map_df_plan_error)
        }
        JoinConstraint::None => LogicalPlanBuilder::from(left)
            .join_on(right, join_type, [])
            .map_err(map_df_plan_error)?
            .build()
            .map_err(map_df_plan_error),
    }
}

fn join_using_key(name: &crate::parser::ast::ObjectName) -> Result<Column, PlannerError> {
    let mut parts = name.0.iter();
    let Some(part) = parts.next() else {
        return Err(PlannerError::InvalidPlan {
            reason: "empty USING column name".to_owned(),
        });
    };
    if parts.next().is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("invalid identifier in USING clause `{name}`"),
        });
    }
    let Some(ident) = part.as_ident() else {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("expected identifier in USING clause, got `{part}`"),
        });
    };
    Ok(Column::from_name(if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }))
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
    planner_context: &QueryPlannerContext<'_>,
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
                // TODO: support table function arguments and other table extensions like DataFusion.
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            }
            if let Some(cte_plan) = planner_context.cte(name)? {
                return apply_relation_alias(cte_plan, alias);
            }
            let table = planner_context.resolve_table(name)?;
            let scan_name = planner_context.table_reference_for_scan(name, alias)?;
            let table_source = planner_context.table_source(table)?;
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
                // TODO: support lateral derived tables with outer query schema tracking.
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported table factor `{factor}`"),
                });
            }
            let mut input = plan_query(subquery, planner_context, Vec::new())?;
            if let Some(alias) = alias {
                input = apply_table_alias(input, alias)?;
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
            build_table_with_joins(table_with_joins, planner_context)
        }
        other => {
            // TODO: support UNNEST and TableFactor::Function following DataFusion relation planning.
            Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported table factor `{other}`"),
            })
        }
    }
}

pub(super) fn apply_table_alias(
    input: DataFusionLogicalPlan,
    alias: &TableAlias,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let input = apply_column_aliases(input, alias)?;
    LogicalPlanBuilder::from(input)
        .alias(alias.name.value.clone())
        .map_err(map_df_plan_error)?
        .build()
        .map_err(map_df_plan_error)
}

fn apply_relation_alias(
    input: DataFusionLogicalPlan,
    alias: &Option<TableAlias>,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    alias
        .as_ref()
        .map(|alias| apply_table_alias(input.clone(), alias))
        .unwrap_or(Ok(input))
}

fn apply_column_aliases(
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
use std::collections::HashSet;
