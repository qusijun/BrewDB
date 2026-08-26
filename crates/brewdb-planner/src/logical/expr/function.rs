use crate::parser::ast::{
    DuplicateTreatment, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments,
    NullTreatment as AstNullTreatment, OrderByExpr, SelectItemQualifiedWildcardKind,
};
use crate::planner::errors::{map_df_plan_error, PlannerError};
use crate::planner::logical::context::QueryPlannerContext;
use datafusion_expr::expr::{AggregateFunction, NullTreatment, ScalarFunction};
use datafusion_expr::planner::{PlannerResult, RawAggregateExpr};
use datafusion_expr::{Expr as DataFusionExpr, SortExpr};

use super::projection::{bind_qualified_wildcard, wildcard_expr};
use super::{bind_expr_with_subqueries, SubqueryPlanner};

pub(super) fn bind_function(
    function: &crate::parser::ast::Function,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let function_args = FunctionArgs::try_new(function)?;
    let args = function_args
        .args
        .iter()
        .map(|arg| bind_function_arg(arg, planner_context, subquery_planner))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = function_args
        .order_by
        .iter()
        .map(|expr| bind_order_by_expr(expr, planner_context, subquery_planner))
        .collect::<Result<Vec<_>, _>>()?;
    let function_name = function.name.to_string();
    if let Some(udf) = planner_context.scalar_function(&function_name) {
        if function_args.filter.is_some()
            || function_args.null_treatment.is_some()
            || !order_by.is_empty()
            || !function_args.within_group.is_empty()
        {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported scalar function shape `{function}`"),
            });
        }
        return Ok(DataFusionExpr::ScalarFunction(ScalarFunction::new_udf(
            udf, args,
        )));
    }
    if let Some(udaf) = planner_context.aggregate_function(&function_name) {
        let filter = function_args
            .filter
            .as_ref()
            .map(|expr| bind_expr_with_subqueries(expr, planner_context, subquery_planner))
            .transpose()?
            .map(Box::new);
        return bind_aggregate_function(
            AggregateFunction::new_udf(
                udaf,
                args,
                function_args.distinct,
                filter,
                order_by,
                function_args.null_treatment,
            ),
            planner_context,
        );
    }
    Err(PlannerError::UnsupportedPlan {
        reason: format!("function `{function_name}` not found in DataFusion function registry"),
    })
}

#[derive(Debug)]
struct FunctionArgs<'a> {
    args: &'a [FunctionArg],
    order_by: Vec<&'a OrderByExpr>,
    filter: Option<&'a crate::parser::ast::Expr>,
    null_treatment: Option<NullTreatment>,
    distinct: bool,
    within_group: &'a [OrderByExpr],
}

impl<'a> FunctionArgs<'a> {
    fn try_new(function: &'a crate::parser::ast::Function) -> Result<Self, PlannerError> {
        if function.over.is_some() || function.parameters != FunctionArguments::None {
            // TODO: align with DataFusion's window function and function parameter planning.
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported function shape `{function}`"),
            });
        }
        let (args, distinct, clauses) = match &function.args {
            FunctionArguments::List(arguments) => (
                arguments.args.as_slice(),
                matches!(
                    arguments.duplicate_treatment,
                    Some(DuplicateTreatment::Distinct)
                ),
                arguments.clauses.as_slice(),
            ),
            FunctionArguments::None => (&[][..], false, &[][..]),
            FunctionArguments::Subquery(query) => {
                // TODO: handle function subquery arguments if DataFusion support is required.
                return Err(PlannerError::UnsupportedPlan {
                    reason: format!("unsupported function subquery argument `{query}`"),
                });
            }
        };
        let mut order_by = Vec::new();
        let mut null_treatment = function.null_treatment.map(bind_null_treatment);
        for clause in clauses {
            match clause {
                FunctionArgumentClause::OrderBy(expressions) => {
                    if !order_by.is_empty() || !function.within_group.is_empty() {
                        return Err(PlannerError::UnsupportedPlan {
                            reason: format!("duplicated function ORDER BY clause `{function}`"),
                        });
                    }
                    order_by.extend(expressions.iter());
                }
                FunctionArgumentClause::IgnoreOrRespectNulls(treatment) => {
                    if null_treatment.is_some() {
                        return Err(PlannerError::UnsupportedPlan {
                            reason: format!("duplicated function null treatment `{function}`"),
                        });
                    }
                    null_treatment = Some(bind_null_treatment(*treatment));
                }
                other => {
                    // TODO: mirror DataFusion's function argument clause handling.
                    return Err(PlannerError::UnsupportedPlan {
                        reason: format!("unsupported function argument clause `{other}`"),
                    });
                }
            }
        }
        Ok(Self {
            args,
            order_by,
            filter: function.filter.as_deref(),
            null_treatment,
            distinct,
            within_group: &function.within_group,
        })
    }
}

fn bind_null_treatment(treatment: AstNullTreatment) -> NullTreatment {
    match treatment {
        AstNullTreatment::IgnoreNulls => NullTreatment::IgnoreNulls,
        AstNullTreatment::RespectNulls => NullTreatment::RespectNulls,
    }
}

fn bind_order_by_expr(
    expr: &OrderByExpr,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<SortExpr, PlannerError> {
    if expr.with_fill.is_some() {
        return Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported function ORDER BY expression `{expr}`"),
        });
    }
    Ok(SortExpr::new(
        bind_expr_with_subqueries(&expr.expr, planner_context, subquery_planner)?,
        expr.options.asc.unwrap_or(true),
        expr.options.nulls_first.unwrap_or(false),
    ))
}

fn bind_aggregate_function(
    function: AggregateFunction,
    planner_context: &QueryPlannerContext<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    let mut aggregate_expr = RawAggregateExpr {
        func: function.func,
        args: function.params.args,
        distinct: function.params.distinct,
        filter: function.params.filter,
        order_by: function.params.order_by,
        null_treatment: function.params.null_treatment,
    };
    for planner in planner_context.expr_planners() {
        match planner
            .plan_aggregate(aggregate_expr)
            .map_err(map_df_plan_error)?
        {
            PlannerResult::Planned(expr) => return Ok(expr),
            PlannerResult::Original(expr) => aggregate_expr = expr,
        }
    }
    Ok(DataFusionExpr::AggregateFunction(
        AggregateFunction::new_udf(
            aggregate_expr.func,
            aggregate_expr.args,
            aggregate_expr.distinct,
            aggregate_expr.filter,
            aggregate_expr.order_by,
            aggregate_expr.null_treatment,
        ),
    ))
}

fn bind_function_arg(
    arg: &FunctionArg,
    planner_context: &QueryPlannerContext<'_>,
    subquery_planner: &mut SubqueryPlanner<'_>,
) -> Result<DataFusionExpr, PlannerError> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => bind_expr_with_subqueries(expr, planner_context, subquery_planner),
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(wildcard_expr()),
        FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(name)) => {
            bind_qualified_wildcard(&SelectItemQualifiedWildcardKind::ObjectName(name.clone()))
        }
        other => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported function argument `{other}`"),
        }),
    }
}
