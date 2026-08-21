use crate::parser::ast::{
    BinaryOperator as AstBinaryOperator, DataType as AstDataType, DuplicateTreatment,
    Expr as AstExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Interval as AstInterval, SelectItem, SelectItemQualifiedWildcardKind, TypedString,
    UnaryOperator as AstUnaryOperator, Value,
};
use crate::planner::errors::PlannerError;
use arrow::compute::kernels::cast_utils::{
    parse_interval_month_day_nano_config, IntervalParseConfig, IntervalUnit,
};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::{ScalarValue, Spans};
use datafusion_expr::expr::{
    AggregateFunction, Cast, Exists, InList, InSubquery, ScalarFunction, WildcardOptions,
};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    col, lit, Between, BinaryExpr, Case, Expr as DataFusionExpr, Like,
    LogicalPlan as DataFusionLogicalPlan, Operator as DataFusionOperator,
    Subquery as DataFusionSubquery,
};
use std::sync::Arc;

type SubqueryPlanner<'a> =
    dyn FnMut(&crate::parser::ast::Query) -> Result<DataFusionLogicalPlan, PlannerError> + 'a;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum QueryGroupBy {
    None,
    All,
    Expressions(Vec<DataFusionExpr>),
}

pub(super) fn bind_group_by_with_subqueries(
    group_by: &GroupByExpr,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<QueryGroupBy, PlannerError> {
    match group_by {
        GroupByExpr::All(_) => Ok(QueryGroupBy::All),
        GroupByExpr::Expressions(expressions, _) if expressions.is_empty() => {
            Ok(QueryGroupBy::None)
        }
        GroupByExpr::Expressions(expressions, _) => Ok(QueryGroupBy::Expressions(
            expressions
                .iter()
                .map(|expr| bind_expr_with_subqueries(expr, function_registry, subquery_planner))
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}

pub(super) fn bind_projection_with_subqueries(
    items: &[SelectItem],
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<Vec<DataFusionExpr>, PlannerError> {
    items
        .iter()
        .map(|item| bind_select_item(item, function_registry, subquery_planner))
        .collect()
}

fn bind_select_item(
    item: &SelectItem,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            bind_expr_with_subqueries(expr, function_registry, subquery_planner)
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            Ok(
                bind_expr_with_subqueries(expr, function_registry, subquery_planner)?
                    .alias(alias.value.clone()),
            )
        }
        SelectItem::ExprWithAliases { expr, aliases } => {
            let Some(alias) = aliases.first() else {
                return Err(PlannerError::InvalidPlan {
                    reason: "projection aliases must not be empty".to_string(),
                });
            };
            Ok(
                bind_expr_with_subqueries(expr, function_registry, subquery_planner)?
                    .alias(alias.value.clone()),
            )
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

#[allow(deprecated)]
pub(super) fn projection_is_passthrough_wildcard(projection: &[DataFusionExpr]) -> bool {
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

pub(super) fn bind_expr(
    expr: &AstExpr,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    bind_expr_with_subqueries(expr, function_registry, &mut unsupported_subquery_planner)
}

pub(super) fn bind_expr_with_subqueries(
    expr: &AstExpr,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match expr {
        AstExpr::Identifier(ident) => Ok(col(ident.to_string())),
        AstExpr::CompoundIdentifier(idents) => Ok(col(idents
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("."))),
        AstExpr::Value(value) => bind_value(value),
        AstExpr::Nested(expr) => {
            bind_expr_with_subqueries(expr, function_registry, subquery_planner)
        }
        AstExpr::BinaryOp { left, op, right } => Ok(DataFusionExpr::BinaryExpr(BinaryExpr::new(
            Box::new(bind_expr_with_subqueries(
                left,
                function_registry,
                subquery_planner,
            )?),
            bind_binary_operator(op)?,
            Box::new(bind_expr_with_subqueries(
                right,
                function_registry,
                subquery_planner,
            )?),
        ))),
        AstExpr::UnaryOp { op, expr } => {
            bind_unary_expr(op, expr, function_registry, subquery_planner)
        }
        AstExpr::IsNull(expr) => Ok(DataFusionExpr::IsNull(Box::new(bind_expr_with_subqueries(
            expr,
            function_registry,
            subquery_planner,
        )?))),
        AstExpr::IsNotNull(expr) => Ok(DataFusionExpr::IsNotNull(Box::new(
            bind_expr_with_subqueries(expr, function_registry, subquery_planner)?,
        ))),
        AstExpr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(DataFusionExpr::Between(Between::new(
            Box::new(bind_expr_with_subqueries(
                expr,
                function_registry,
                subquery_planner,
            )?),
            *negated,
            Box::new(bind_expr_with_subqueries(
                low,
                function_registry,
                subquery_planner,
            )?),
            Box::new(bind_expr_with_subqueries(
                high,
                function_registry,
                subquery_planner,
            )?),
        ))),
        AstExpr::InList {
            expr,
            list,
            negated,
        } => Ok(DataFusionExpr::InList(InList::new(
            Box::new(bind_expr_with_subqueries(
                expr,
                function_registry,
                subquery_planner,
            )?),
            list.iter()
                .map(|expr| bind_expr_with_subqueries(expr, function_registry, subquery_planner))
                .collect::<Result<Vec<_>, _>>()?,
            *negated,
        ))),
        AstExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let subquery = plan_subquery(subquery, subquery_planner)?;
            validate_single_column_subquery(&subquery)?;
            Ok(DataFusionExpr::InSubquery(InSubquery::new(
                Box::new(bind_expr_with_subqueries(
                    expr,
                    function_registry,
                    subquery_planner,
                )?),
                subquery,
                *negated,
            )))
        }
        AstExpr::Exists { subquery, negated } => Ok(DataFusionExpr::Exists(Exists::new(
            plan_subquery(subquery, subquery_planner)?,
            *negated,
        ))),
        AstExpr::Subquery(subquery) => {
            let subquery = plan_subquery(subquery, subquery_planner)?;
            validate_single_column_subquery(&subquery)?;
            Ok(DataFusionExpr::ScalarSubquery(subquery))
        }
        AstExpr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            false,
            function_registry,
            subquery_planner,
        ),
        AstExpr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => bind_like_expr(
            *negated,
            *any,
            expr,
            pattern,
            escape_char.as_ref(),
            true,
            function_registry,
            subquery_planner,
        ),
        AstExpr::TypedString(typed) => bind_typed_string(typed),
        AstExpr::Interval(interval) => bind_interval(interval),
        AstExpr::Extract { field, expr, .. } => bind_scalar_function(
            "date_part",
            vec![
                lit(field.to_string().to_ascii_lowercase()),
                bind_expr_with_subqueries(expr, function_registry, subquery_planner)?,
            ],
            function_registry,
        ),
        AstExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => bind_substring(
            expr,
            substring_from,
            substring_for,
            function_registry,
            subquery_planner,
        ),
        AstExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => Ok(DataFusionExpr::Case(Case::new(
            operand
                .as_ref()
                .map(|expr| {
                    bind_expr_with_subqueries(expr, function_registry, subquery_planner)
                        .map(Box::new)
                })
                .transpose()?,
            conditions
                .iter()
                .map(|when| {
                    Ok((
                        Box::new(bind_expr_with_subqueries(
                            &when.condition,
                            function_registry,
                            subquery_planner,
                        )?),
                        Box::new(bind_expr_with_subqueries(
                            &when.result,
                            function_registry,
                            subquery_planner,
                        )?),
                    ))
                })
                .collect::<Result<Vec<_>, PlannerError>>()?,
            else_result
                .as_ref()
                .map(|expr| {
                    bind_expr_with_subqueries(expr, function_registry, subquery_planner)
                        .map(Box::new)
                })
                .transpose()?,
        ))),
        AstExpr::Function(function) => bind_function(function, function_registry, subquery_planner),
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported expression `{other}`"),
        }),
    }
}

fn unsupported_subquery_planner(
    subquery: &crate::parser::ast::Query,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    Err(PlannerError::UnsupportedPlan {
        reason: format!("unsupported subquery `{subquery}`"),
    })
}

fn plan_subquery(
    subquery: &crate::parser::ast::Query,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionSubquery, PlannerError> {
    let subquery = subquery_planner(subquery)?;
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Ok(DataFusionSubquery {
        subquery: Arc::new(subquery),
        outer_ref_columns,
        spans: Spans::new(),
    })
}

fn validate_single_column_subquery(subquery: &DataFusionSubquery) -> Result<(), PlannerError> {
    let column_count = subquery.subquery.schema().fields().len();
    if column_count == 1 {
        return Ok(());
    }
    Err(PlannerError::Plan {
        reason: format!("subquery must return exactly one column, got {column_count}"),
    })
}

fn bind_scalar_function(
    name: &str,
    args: Vec<DataFusionExpr>,
    function_registry: &dyn FunctionRegistry,
) -> Result<DataFusionExpr, PlannerError> {
    function_registry
        .udf(name)
        .map(|udf| DataFusionExpr::ScalarFunction(ScalarFunction::new_udf(udf, args)))
        .map_err(|_| PlannerError::UnsupportedPlan {
            reason: format!("function `{name}` not found in DataFusion function registry"),
        })
}

fn bind_substring(
    expr: &AstExpr,
    substring_from: &Option<Box<AstExpr>>,
    substring_for: &Option<Box<AstExpr>>,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let mut args = vec![bind_expr_with_subqueries(
        expr,
        function_registry,
        subquery_planner,
    )?];
    match (substring_from, substring_for) {
        (Some(from), Some(for_expr)) => {
            args.push(bind_expr_with_subqueries(
                from,
                function_registry,
                subquery_planner,
            )?);
            args.push(bind_expr_with_subqueries(
                for_expr,
                function_registry,
                subquery_planner,
            )?);
        }
        (Some(from), None) => {
            args.push(bind_expr_with_subqueries(
                from,
                function_registry,
                subquery_planner,
            )?);
        }
        (None, Some(for_expr)) => {
            args.push(lit(1_i64));
            args.push(bind_expr_with_subqueries(
                for_expr,
                function_registry,
                subquery_planner,
            )?);
        }
        (None, None) => {
            return Err(PlannerError::InvalidPlan {
                reason: format!("substring without FROM or FOR is not valid: `{expr}`"),
            });
        }
    }
    bind_scalar_function("substring", args.clone(), function_registry)
        .or_else(|_| bind_scalar_function("substr", args, function_registry))
}

fn bind_like_expr(
    negated: bool,
    any: bool,
    expr: &AstExpr,
    pattern: &AstExpr,
    escape_char: Option<&crate::parser::ast::ValueWithSpan>,
    case_insensitive: bool,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    if any {
        return Err(PlannerError::UnsupportedPlan {
            reason: "ANY in LIKE expression is not supported yet".to_string(),
        });
    }
    let escape_char = match escape_char.map(|value| &value.value) {
        Some(Value::SingleQuotedString(value)) if value.chars().count() == 1 => {
            value.chars().next()
        }
        Some(value) => {
            return Err(PlannerError::InvalidPlan {
                reason: format!(
                    "LIKE escape character must be a single quoted character, got `{value}`"
                ),
            });
        }
        None => None,
    };
    Ok(DataFusionExpr::Like(Like::new(
        negated,
        Box::new(bind_expr_with_subqueries(
            expr,
            function_registry,
            subquery_planner,
        )?),
        Box::new(bind_expr_with_subqueries(
            pattern,
            function_registry,
            subquery_planner,
        )?),
        escape_char,
        case_insensitive,
    )))
}

fn bind_interval(interval: &AstInterval) -> Result<DataFusionExpr, PlannerError> {
    if interval.leading_precision.is_some()
        || interval.last_field.is_some()
        || interval.fractional_seconds_precision.is_some()
    {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported interval expression `{interval}`"),
        });
    }
    let mut value = interval_literal(interval.value.as_ref(), false)?;
    if let Some(leading_field) = &interval.leading_field {
        value = format!("{value} {leading_field}");
    }
    let parsed = parse_interval_month_day_nano_config(
        &value,
        IntervalParseConfig::new(IntervalUnit::Second),
    )
    .map_err(|err| PlannerError::InvalidPlan {
        reason: format!("invalid interval literal `{interval}`: {err}"),
    })?;
    Ok(lit(ScalarValue::IntervalMonthDayNano(Some(parsed))))
}

fn interval_literal(expr: &AstExpr, negative: bool) -> Result<String, PlannerError> {
    let value = match expr {
        AstExpr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => value.clone(),
            Value::Number(value, long) if !long => value.clone(),
            Value::Number(_, _) => {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported long interval literal `{expr}`"),
                });
            }
            other => {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported interval literal `{other}`"),
                });
            }
        },
        AstExpr::UnaryOp { op, expr } => match op {
            AstUnaryOperator::Minus => interval_literal(expr, !negative)?,
            AstUnaryOperator::Plus => interval_literal(expr, negative)?,
            other => {
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported interval unary operator `{other}`"),
                });
            }
        },
        other => {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported interval argument `{other}`"),
            });
        }
    };
    Ok(if negative { format!("-{value}") } else { value })
}

fn bind_typed_string(typed: &TypedString) -> Result<DataFusionExpr, PlannerError> {
    let Some(value) = typed.value.clone().into_string() else {
        return Err(PlannerError::InvalidPlan {
            reason: format!("typed literal `{typed}` requires a string payload"),
        });
    };
    let data_type = match &typed.data_type {
        AstDataType::Date => ArrowDataType::Date32,
        other => {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported typed literal data type `{other}`"),
            });
        }
    };
    Ok(DataFusionExpr::Cast(Cast::new(
        Box::new(lit(value)),
        data_type,
    )))
}

fn bind_unary_expr(
    op: &AstUnaryOperator,
    expr: &AstExpr,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let expr = bind_expr_with_subqueries(expr, function_registry, subquery_planner)?;
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

fn bind_value(value: &crate::parser::ast::ValueWithSpan) -> Result<DataFusionExpr, PlannerError> {
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
    function: &crate::parser::ast::Function,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
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
            .map(|arg| bind_function_arg(arg, function_registry, subquery_planner))
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
            .map(|expr| bind_expr_with_subqueries(expr, function_registry, subquery_planner))
            .transpose()?
            .map(Box::new);
        if function.null_treatment.is_some() {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported aggregate function null treatment `{function}`"),
            });
        }
        return Ok(plan_aggregate_function(
            &function_name,
            AggregateFunction::new_udf(udaf, args, distinct, filter, Vec::new(), None),
        ));
    }
    Err(PlannerError::UnsupportedPlan {
        reason: format!("function `{function_name}` not found in DataFusion function registry"),
    })
}

#[allow(deprecated)]
fn plan_aggregate_function(function_name: &str, function: AggregateFunction) -> DataFusionExpr {
    let expr = DataFusionExpr::AggregateFunction(function);
    if function_name.eq_ignore_ascii_case("count") {
        match &expr {
            DataFusionExpr::AggregateFunction(function)
                if function.params.args.is_empty()
                    || matches!(
                        function.params.args.as_slice(),
                        [DataFusionExpr::Wildcard { .. }]
                    ) =>
            {
                return DataFusionExpr::AggregateFunction(AggregateFunction::new_udf(
                    function.func.clone(),
                    vec![DataFusionExpr::Literal(COUNT_STAR_EXPANSION, None)],
                    function.params.distinct,
                    function.params.filter.clone(),
                    function.params.order_by.clone(),
                    function.params.null_treatment,
                ));
            }
            _ => {}
        }
    }
    expr
}

fn bind_function_arg(
    arg: &FunctionArg,
    function_registry: &dyn FunctionRegistry,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => bind_expr_with_subqueries(expr, function_registry, subquery_planner),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(wildcard_expr()),
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => {
            bind_qualified_wildcard(&SelectItemQualifiedWildcardKind::ObjectName(name.clone()))
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported function argument `{other}`"),
        }),
    }
}
