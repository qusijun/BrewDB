use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator};
use paimon::spec::{Datum, Predicate, PredicateBuilder, PredicateOperator};

use paimon::spec::DataField;

pub(crate) fn filter_pushdown_status(
    fields: &[DataField],
    partition_keys: &[String],
    filter: &Expr,
) -> Option<(Predicate, bool)> {
    let predicate = expr_to_predicate(fields, filter)?;
    let exact = predicate_uses_only_partition_keys(&predicate, partition_keys);
    Some((predicate, exact))
}

pub(crate) fn filter_predicates(fields: &[DataField], filters: &[Expr]) -> Vec<Predicate> {
    filters
        .iter()
        .filter_map(|filter| expr_to_predicate(fields, filter))
        .collect()
}

fn expr_to_predicate(fields: &[DataField], expr: &Expr) -> Option<Predicate> {
    let builder = PredicateBuilder::new(fields);
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                let left = expr_to_predicate(fields, binary.left.as_ref())?;
                let right = expr_to_predicate(fields, binary.right.as_ref())?;
                Some(Predicate::and(vec![left, right]))
            }
            Operator::Or => {
                let left = expr_to_predicate(fields, binary.left.as_ref())?;
                let right = expr_to_predicate(fields, binary.right.as_ref())?;
                Some(Predicate::or(vec![left, right]))
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                let (field, literal, op) =
                    binary_comparison(binary.left.as_ref(), binary.op, binary.right.as_ref())?;
                build_comparison(&builder, field, op, literal)
            }
            _ => None,
        },
        Expr::Not(expr) => expr_to_predicate(fields, expr).map(Predicate::negate),
        Expr::IsNull(expr) => column_name(expr).and_then(|field| builder.is_null(field).ok()),
        Expr::IsNotNull(expr) => {
            column_name(expr).and_then(|field| builder.is_not_null(field).ok())
        }
        Expr::Between(between) => {
            let field = column_name(between.expr.as_ref())?;
            let lower = literal_value(between.low.as_ref())?;
            let upper = literal_value(between.high.as_ref())?;
            let lower_pred = builder.greater_or_equal(field, lower).ok()?;
            let upper_pred = builder.less_or_equal(field, upper).ok()?;
            let predicate = Predicate::and(vec![lower_pred, upper_pred]);
            Some(if between.negated {
                Predicate::negate(predicate)
            } else {
                predicate
            })
        }
        Expr::InList(in_list) => {
            let field = column_name(in_list.expr.as_ref())?;
            let literals = in_list
                .list
                .iter()
                .map(literal_value)
                .collect::<Option<Vec<_>>>()?;
            if in_list.negated {
                builder.is_not_in(field, literals).ok()
            } else {
                builder.is_in(field, literals).ok()
            }
        }
        Expr::Cast(cast) => expr_to_predicate(fields, cast.expr.as_ref()),
        Expr::TryCast(cast) => expr_to_predicate(fields, cast.expr.as_ref()),
        _ => None,
    }
}

fn build_comparison(
    builder: &PredicateBuilder,
    field: &str,
    op: PredicateOperator,
    literal: Datum,
) -> Option<Predicate> {
    match op {
        PredicateOperator::Eq => builder.equal(field, literal).ok(),
        PredicateOperator::NotEq => builder.not_equal(field, literal).ok(),
        PredicateOperator::Lt => builder.less_than(field, literal).ok(),
        PredicateOperator::LtEq => builder.less_or_equal(field, literal).ok(),
        PredicateOperator::Gt => builder.greater_than(field, literal).ok(),
        PredicateOperator::GtEq => builder.greater_or_equal(field, literal).ok(),
        _ => None,
    }
}

fn binary_comparison<'a>(
    left: &'a Expr,
    op: Operator,
    right: &'a Expr,
) -> Option<(&'a str, Datum, PredicateOperator)> {
    if let (Some(field), Some(literal)) = (column_name(left), literal_value(right)) {
        return Some((field, literal, operator_to_predicate_operator(op)));
    }
    if let (Some(field), Some(literal)) = (column_name(right), literal_value(left)) {
        return Some((field, literal, reverse_operator(op)));
    }
    if let Some(field) = column_name(left) {
        if matches!(op, Operator::Eq | Operator::NotEq) {
            if let Some(literal) = literal_value(right) {
                return Some((field, literal, operator_to_predicate_operator(op)));
            }
        }
    }
    if let Some(field) = column_name(right) {
        if matches!(op, Operator::Eq | Operator::NotEq) {
            if let Some(literal) = literal_value(left) {
                return Some((field, literal, reverse_operator(op)));
            }
        }
    }
    None
}

fn operator_to_predicate_operator(op: Operator) -> PredicateOperator {
    match op {
        Operator::Eq => PredicateOperator::Eq,
        Operator::NotEq => PredicateOperator::NotEq,
        Operator::Lt => PredicateOperator::Lt,
        Operator::LtEq => PredicateOperator::LtEq,
        Operator::Gt => PredicateOperator::Gt,
        Operator::GtEq => PredicateOperator::GtEq,
        _ => unreachable!("unsupported operator for predicate translation: {op:?}"),
    }
}

fn reverse_operator(op: Operator) -> PredicateOperator {
    match op {
        Operator::Lt => PredicateOperator::Gt,
        Operator::LtEq => PredicateOperator::GtEq,
        Operator::Gt => PredicateOperator::Lt,
        Operator::GtEq => PredicateOperator::LtEq,
        _ => operator_to_predicate_operator(op),
    }
}

fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(cast) => column_name(cast.expr.as_ref()),
        Expr::TryCast(cast) => column_name(cast.expr.as_ref()),
        _ => None,
    }
}

fn literal_value(expr: &Expr) -> Option<Datum> {
    match expr {
        Expr::Literal(value, _) => scalar_to_datum(value),
        Expr::Cast(cast) => literal_value(cast.expr.as_ref()),
        Expr::TryCast(cast) => literal_value(cast.expr.as_ref()),
        _ => None,
    }
}

fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::Bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Datum::TinyInt(*v)),
        ScalarValue::Int16(Some(v)) => Some(Datum::SmallInt(*v)),
        ScalarValue::Int32(Some(v)) => Some(Datum::Int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::Long(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Datum::Long(i64::from(*v))),
        ScalarValue::UInt16(Some(v)) => Some(Datum::Long(i64::from(*v))),
        ScalarValue::UInt32(Some(v)) => Some(Datum::Long(i64::from(*v))),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(Datum::Long),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Some(Datum::String(v.clone())),
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Some(Datum::Bytes(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::Date(*v)),
        ScalarValue::Time32Millisecond(Some(v)) => Some(Datum::Time(*v)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(Datum::Timestamp {
            millis: *v,
            nanos: 0,
        }),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(Datum::Timestamp {
            millis: *v / 1_000,
            nanos: ((*v % 1_000) * 1_000) as i32,
        }),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(Datum::Timestamp {
            millis: *v / 1_000_000,
            nanos: ((*v % 1_000_000) * 1_000) as i32,
        }),
        ScalarValue::Decimal128(Some(v), precision, scale) => Some(Datum::Decimal {
            unscaled: *v,
            precision: u32::from(*precision),
            scale: u32::try_from(*scale).ok()?,
        }),
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Float16(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Decimal32(None, ..)
        | ScalarValue::Decimal64(None, ..)
        | ScalarValue::Decimal128(None, ..)
        | ScalarValue::Decimal256(None, ..)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::FixedSizeBinary(_, None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::Date32(None)
        | ScalarValue::Date64(None)
        | ScalarValue::Time32Second(None)
        | ScalarValue::Time32Millisecond(None)
        | ScalarValue::Time64Microsecond(None)
        | ScalarValue::Time64Nanosecond(None)
        | ScalarValue::TimestampSecond(None, _)
        | ScalarValue::TimestampMillisecond(None, _)
        | ScalarValue::TimestampMicrosecond(None, _)
        | ScalarValue::TimestampNanosecond(None, _)
        | ScalarValue::IntervalYearMonth(None)
        | ScalarValue::IntervalDayTime(None)
        | ScalarValue::IntervalMonthDayNano(None)
        | ScalarValue::DurationSecond(None)
        | ScalarValue::DurationMillisecond(None)
        | ScalarValue::DurationMicrosecond(None)
        | ScalarValue::DurationNanosecond(None)
        | ScalarValue::FixedSizeList(_)
        | ScalarValue::List(_)
        | ScalarValue::LargeList(_)
        | ScalarValue::ListView(_)
        | ScalarValue::LargeListView(_)
        | ScalarValue::Struct(_)
        | ScalarValue::Map(_)
        | ScalarValue::Union(_, _, _)
        | ScalarValue::Dictionary(_, _)
        | ScalarValue::RunEndEncoded(_, _, _) => None,
        ScalarValue::Float16(Some(v)) => Some(Datum::Double(f64::from(v.to_f32()))),
        ScalarValue::Float32(Some(v)) => Some(Datum::Float(*v)),
        ScalarValue::Float64(Some(v)) => Some(Datum::Double(*v)),
        ScalarValue::Decimal32(Some(v), precision, scale) => Some(Datum::Decimal {
            unscaled: i128::from(*v),
            precision: u32::from(*precision),
            scale: u32::try_from(*scale).ok()?,
        }),
        ScalarValue::Decimal64(Some(v), precision, scale) => Some(Datum::Decimal {
            unscaled: i128::from(*v),
            precision: u32::from(*precision),
            scale: u32::try_from(*scale).ok()?,
        }),
        ScalarValue::Decimal256(Some(_), _, _) => None,
        ScalarValue::Date64(Some(v)) => Some(Datum::Date((*v / 86_400_000) as i32)),
        ScalarValue::Time32Second(Some(v)) => Some(Datum::Time(*v * 1_000)),
        ScalarValue::Time64Microsecond(Some(v)) => Some(Datum::Time((*v / 1_000) as i32)),
        ScalarValue::Time64Nanosecond(Some(v)) => Some(Datum::Time((*v / 1_000_000) as i32)),
        ScalarValue::FixedSizeBinary(_, Some(v)) => Some(Datum::Bytes(v.clone())),
        _ => None,
    }
}

fn predicate_uses_only_partition_keys(predicate: &Predicate, partition_keys: &[String]) -> bool {
    match predicate {
        Predicate::Leaf { column, .. } => partition_keys.iter().any(|key| key == column),
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .all(|child| predicate_uses_only_partition_keys(child, partition_keys)),
        Predicate::Not(child) => predicate_uses_only_partition_keys(child, partition_keys),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{Expr, Operator, lit};
    use paimon::spec::{DataType, IntType, VarCharType};

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "dt".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
        ]
    }

    #[test]
    fn translates_simple_comparison() {
        let expr = Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
            Box::new(datafusion_expr::col("id")),
            Operator::Gt,
            Box::new(lit(10_i32)),
        ));

        let predicate = filter_predicates(&fields(), &[expr]);

        assert_eq!(predicate.len(), 1);
        assert!(matches!(
            predicate[0],
            Predicate::Leaf {
                op: PredicateOperator::Gt,
                ..
            }
        ));
    }

    #[test]
    fn partition_key_filter_is_exact() {
        let expr = Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
            Box::new(datafusion_expr::col("dt")),
            Operator::Eq,
            Box::new(lit("2024-01-01")),
        ));

        let (_, exact) = filter_pushdown_status(&fields(), &["dt".to_string()], &expr).unwrap();

        assert!(exact);
    }
}
