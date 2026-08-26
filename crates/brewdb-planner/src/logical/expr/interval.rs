use crate::parser::ast::{
    Expr as AstExpr, Interval as AstInterval, UnaryOperator as AstUnaryOperator, Value,
};
use crate::planner::errors::PlannerError;
use arrow::compute::kernels::cast_utils::{
    parse_interval_month_day_nano_config, IntervalParseConfig, IntervalUnit,
};
use datafusion_common::ScalarValue;
use datafusion_expr::{lit, Expr as DataFusionExpr};

pub(super) fn bind_interval(interval: &AstInterval) -> Result<DataFusionExpr, PlannerError> {
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
