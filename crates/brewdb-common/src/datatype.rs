//! Shared logical data types used across catalog, planning, and execution.

use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

use crate::errors::CommonError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Double,
    Binary,
    Date,
    Time {
        precision: u32,
    },
    Timestamp {
        precision: u32,
        with_time_zone: bool,
    },
    Decimal {
        precision: u32,
        scale: u32,
    },
    String,
}

impl DataType {
    pub fn to_arrow_data_type(&self) -> Result<ArrowDataType, CommonError> {
        Ok(match self {
            Self::Boolean => ArrowDataType::Boolean,
            Self::Int8 => ArrowDataType::Int8,
            Self::Int16 => ArrowDataType::Int16,
            Self::Int32 => ArrowDataType::Int32,
            Self::Int64 => ArrowDataType::Int64,
            Self::Float32 => ArrowDataType::Float32,
            Self::Double => ArrowDataType::Float64,
            Self::Binary => ArrowDataType::Binary,
            Self::Date => ArrowDataType::Date32,
            Self::Time { precision } => match precision {
                0..=3 => ArrowDataType::Time32(TimeUnit::Millisecond),
                _ => ArrowDataType::Time64(TimeUnit::Microsecond),
            },
            Self::Timestamp {
                precision,
                with_time_zone,
            } => {
                let unit = match precision {
                    0 => TimeUnit::Second,
                    1..=3 => TimeUnit::Millisecond,
                    4..=6 => TimeUnit::Microsecond,
                    _ => TimeUnit::Nanosecond,
                };
                ArrowDataType::Timestamp(unit, with_time_zone.then(|| "UTC".into()))
            }
            Self::Decimal { precision, scale } => ArrowDataType::Decimal128(
                u8::try_from(*precision).map_err(|_| CommonError::SchemaConversionFailed {
                    reason: format!(
                        "decimal precision `{precision}` exceeds Arrow Decimal128 range"
                    ),
                })?,
                i8::try_from(*scale).map_err(|_| CommonError::SchemaConversionFailed {
                    reason: format!("decimal scale `{scale}` exceeds Arrow Decimal128 range"),
                })?,
            ),
            Self::String => ArrowDataType::Utf8,
        })
    }

    pub fn from_arrow_data_type(data_type: &ArrowDataType) -> Result<Self, CommonError> {
        match data_type {
            ArrowDataType::Boolean => Ok(Self::Boolean),
            ArrowDataType::Int8 => Ok(Self::Int8),
            ArrowDataType::Int16 => Ok(Self::Int16),
            ArrowDataType::Int32 => Ok(Self::Int32),
            ArrowDataType::Int64 => Ok(Self::Int64),
            ArrowDataType::Float32 => Ok(Self::Float32),
            ArrowDataType::Float64 => Ok(Self::Double),
            ArrowDataType::Binary => Ok(Self::Binary),
            ArrowDataType::Date32 => Ok(Self::Date),
            ArrowDataType::Utf8 => Ok(Self::String),
            ArrowDataType::Time32(TimeUnit::Second) => Ok(Self::Time { precision: 0 }),
            ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(Self::Time { precision: 3 }),
            ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(Self::Time { precision: 6 }),
            ArrowDataType::Time64(TimeUnit::Nanosecond) => Ok(Self::Time { precision: 9 }),
            ArrowDataType::Timestamp(unit, timezone) => Ok(Self::Timestamp {
                precision: match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                },
                with_time_zone: timezone.is_some(),
            }),
            ArrowDataType::Decimal128(precision, scale) => Ok(Self::Decimal {
                precision: u32::from(*precision),
                scale: u32::try_from(*scale).map_err(|_| CommonError::SchemaConversionFailed {
                    reason: format!(
                        "negative Arrow decimal scale `{scale}` cannot convert to BrewDB decimal"
                    ),
                })?,
            }),
            other => Err(CommonError::SchemaConversionFailed {
                reason: format!("unsupported Arrow data type `{other}`"),
            }),
        }
    }
}
