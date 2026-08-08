//! Shared schema types used across catalog, planning, and execution boundaries.

use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl SchemaField {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn to_arrow_field(&self) -> Result<ArrowField, CommonError> {
        Ok(ArrowField::new(
            &self.name,
            self.data_type.to_arrow_data_type()?,
            self.nullable,
        ))
    }

    pub fn from_arrow_field(field: &ArrowField) -> Result<Self, CommonError> {
        Ok(Self {
            name: field.name().to_owned(),
            data_type: DataType::from_arrow_data_type(field.data_type())?,
            nullable: field.is_nullable(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub fields: Vec<SchemaField>,
}

impl TableSchema {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        Self { fields }
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, CommonError> {
        Ok(ArrowSchema::new(
            self.fields
                .iter()
                .map(SchemaField::to_arrow_field)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    pub fn to_arrow_schema_ref(&self) -> Result<Arc<ArrowSchema>, CommonError> {
        Ok(Arc::new(self.to_arrow_schema()?))
    }

    pub fn from_arrow_schema(schema: &ArrowSchema) -> Result<Self, CommonError> {
        Ok(Self::new(
            schema
                .fields()
                .iter()
                .map(|field| SchemaField::from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
    };

    use super::{DataType, SchemaField, TableSchema};

    #[test]
    fn brewdb_schema_round_trips_through_arrow_schema() {
        let schema = TableSchema::new(vec![
            SchemaField::new("id", DataType::Int64).with_nullable(false),
            SchemaField::new(
                "event_time",
                DataType::Timestamp {
                    precision: 6,
                    with_time_zone: true,
                },
            ),
            SchemaField::new(
                "amount",
                DataType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            )
            .with_nullable(false),
        ]);

        let arrow_schema = schema.to_arrow_schema().unwrap();
        let round_trip = TableSchema::from_arrow_schema(&arrow_schema).unwrap();

        assert_eq!(round_trip, schema);
    }

    #[test]
    fn arrow_schema_round_trips_through_brewdb_schema() {
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new(
                "ts",
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            ArrowField::new("payload", ArrowDataType::Binary, true),
        ]);

        let brewdb_schema = TableSchema::from_arrow_schema(&arrow_schema).unwrap();
        let restored_arrow_schema = brewdb_schema.to_arrow_schema().unwrap();

        assert_eq!(restored_arrow_schema, arrow_schema);
    }

    #[test]
    fn unsupported_arrow_data_type_returns_conversion_error() {
        let error = DataType::from_arrow_data_type(&ArrowDataType::List(Arc::new(
            ArrowField::new("item", ArrowDataType::Int32, true),
        )))
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("schema conversion failed: unsupported Arrow data type")
        );
    }
}
