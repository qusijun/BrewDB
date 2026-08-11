use crate::errors::PlannerError;
use brewdb_sql_parser::ast::{
    BinaryOperator as AstBinaryOperator, DuplicateTreatment, Expr as AstExpr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem, SelectItemQualifiedWildcardKind,
    UnaryOperator as AstUnaryOperator, Value,
};
use datafusion_common::ScalarValue;
use datafusion_expr::expr::{AggregateFunction, ScalarFunction, WildcardOptions};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    BinaryExpr, Expr as DataFusionExpr, Operator as DataFusionOperator, col, lit,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum QueryGroupBy {
    None,
    All,
    Expressions(Vec<DataFusionExpr>),
}

pub(super) fn bind_group_by(
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

pub(super) fn bind_projection(
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
    value: &brewdb_sql_parser::ast::ValueWithSpan,
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
    function: &brewdb_sql_parser::ast::Function,
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
