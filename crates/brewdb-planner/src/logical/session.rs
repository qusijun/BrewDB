use crate::parser::ast::{Expr, ObjectName, Set, Use, Value, ValueWithSpan};
use crate::planner::PlannerError;
use datafusion_expr::{
    LogicalPlan as DataFusionLogicalPlan, SetVariable, Statement as DataFusionStatement,
};

use crate::planner::logical::{
    object_name_to_string, qualify_database_name, LogicalPlanningSession,
};

pub(crate) fn bind_set_statement(
    set: &Set,
    _session: LogicalPlanningSession,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    match set {
        Set::SingleAssignment {
            scope,
            variable,
            values,
            ..
        } => {
            let value =
                values
                    .first()
                    .map(expr_to_string)
                    .ok_or_else(|| PlannerError::InvalidPlan {
                        reason: "SET statement must carry at least one value".to_string(),
                    })?;
            let _ = scope;
            Ok(DataFusionLogicalPlan::Statement(
                DataFusionStatement::SetVariable(SetVariable {
                    variable: object_name_to_string(variable),
                    value,
                }),
            ))
        }
        Set::SetTimeZone { local, value } => {
            let _ = local;
            Ok(DataFusionLogicalPlan::Statement(
                DataFusionStatement::SetVariable(SetVariable {
                    variable: "timezone".to_string(),
                    value: expr_to_string(value),
                }),
            ))
        }
        _ => Err(PlannerError::UnsupportedPlan {
            reason: format!("unsupported SET statement `{set}`"),
        }),
    }
}

pub(crate) fn bind_use_statement(
    session: LogicalPlanningSession,
    use_stmt: &Use,
) -> Result<DataFusionLogicalPlan, PlannerError> {
    let database_name = match use_stmt {
        Use::Object(name) | Use::Database(name) | Use::Schema(name) => {
            qualify_database_from_use(&session, name)?
        }
        Use::Default => session.database_name,
        _ => {
            return Err(PlannerError::UnsupportedPlan {
                reason: format!("unsupported USE statement `{use_stmt}`"),
            });
        }
    };

    Ok(DataFusionLogicalPlan::Statement(
        DataFusionStatement::SetVariable(SetVariable {
            variable: "database".to_string(),
            value: format!("{}.{}", session.catalog_name, database_name),
        }),
    ))
}

fn qualify_database_from_use(
    session: &LogicalPlanningSession,
    name: &ObjectName,
) -> Result<String, PlannerError> {
    let (_, database_name) = qualify_database_name(session, name)?;
    Ok(database_name)
}

fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => value_to_string(value),
        _ => expr.to_string(),
    }
}

fn value_to_string(value: &ValueWithSpan) -> String {
    match &value.value {
        Value::SingleQuotedString(inner)
        | Value::DoubleQuotedString(inner)
        | Value::EscapedStringLiteral(inner)
        | Value::NationalStringLiteral(inner)
        | Value::HexStringLiteral(inner)
        | Value::SingleQuotedByteStringLiteral(inner)
        | Value::DoubleQuotedByteStringLiteral(inner)
        | Value::SingleQuotedRawStringLiteral(inner)
        | Value::DoubleQuotedRawStringLiteral(inner)
        | Value::TripleSingleQuotedString(inner)
        | Value::TripleDoubleQuotedString(inner)
        | Value::TripleSingleQuotedRawStringLiteral(inner)
        | Value::TripleDoubleQuotedRawStringLiteral(inner)
        | Value::UnicodeStringLiteral(inner)
        | Value::TripleSingleQuotedByteStringLiteral(inner)
        | Value::TripleDoubleQuotedByteStringLiteral(inner) => inner.clone(),
        _ => value.to_string(),
    }
}
