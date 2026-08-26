use crate::parser::ast::{DataType as AstDataType, TypedString, Value, ValueWithSpan};
use crate::planner::errors::PlannerError;
use arrow::datatypes::DataType as ArrowDataType;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::Cast;
use datafusion_expr::{lit, Expr as DataFusionExpr};

pub(super) fn bind_value(value: &ValueWithSpan) -> Result<DataFusionExpr, PlannerError> {
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

pub(super) fn bind_typed_string(typed: &TypedString) -> Result<DataFusionExpr, PlannerError> {
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
