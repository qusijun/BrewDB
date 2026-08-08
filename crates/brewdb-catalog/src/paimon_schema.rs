//! Paimon <-> BrewDB schema conversion helpers kept local to brewdb-catalog.

use brewdb_common::schema::{DataType, SchemaField, TableSchema};
use paimon::spec::{
    BigIntType, BooleanType, DataType as PaimonDataType, DateType, DecimalType, DoubleType,
    FloatType, IntType, LocalZonedTimestampType, Schema, SchemaChange, SmallIntType, TimeType,
    TimestampType, TinyIntType, VarBinaryType, VarCharType,
};

use crate::errors::CatalogError;
use crate::requests::{AlterTableOperation, CreateTableRequest};
use crate::storage_format_schema::StorageFormatSchemaAdapter;

pub struct PaimonSchemaAdapter;

impl StorageFormatSchemaAdapter for PaimonSchemaAdapter {
    type FormatDataType = PaimonDataType;
    type CreateSchema = Schema;
    type FormatTableSchema = paimon::spec::TableSchema;
    type AlterChange = SchemaChange;

    fn build_schema(request: &CreateTableRequest) -> Result<Self::CreateSchema, CatalogError> {
        let mut builder = Schema::builder();
        for column in &request.table_schema.fields {
            builder = builder.column(&column.name, Self::brewdb_field_to_format_type(column)?);
        }
        for (key, value) in &request.table_options {
            builder = builder.option(key.clone(), value.clone());
        }
        if let Some(table_location) = &request.table_location {
            builder = builder.option("path", table_location.clone());
        }
        builder.build().map_err(map_paimon_backend_error)
    }

    fn alter_operation_to_change(
        operation: &AlterTableOperation,
    ) -> Result<Self::AlterChange, CatalogError> {
        match operation {
            AlterTableOperation::AddColumn(column) => Ok(SchemaChange::add_column(
                column.name.clone(),
                Self::brewdb_field_to_format_type(column)?,
            )),
            AlterTableOperation::DropColumn { column_name } => {
                Ok(SchemaChange::drop_column(column_name.clone()))
            }
            AlterTableOperation::RenameColumn { old_name, new_name } => Ok(
                SchemaChange::rename_column(old_name.clone(), new_name.clone()),
            ),
            AlterTableOperation::AlterColumnType {
                column_name,
                data_type,
            } => Ok(SchemaChange::update_column_type(
                column_name.clone(),
                Self::brewdb_field_to_format_type(&SchemaField::new(
                    column_name,
                    data_type.clone(),
                ))?,
            )),
            AlterTableOperation::SetTableOption { key, value } => {
                Ok(SchemaChange::set_option(key.clone(), value.clone()))
            }
            AlterTableOperation::RemoveTableOption { key } => {
                Ok(SchemaChange::remove_option(key.clone()))
            }
        }
    }

    fn table_schema_to_brewdb(
        schema: &Self::FormatTableSchema,
    ) -> Result<TableSchema, CatalogError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                Ok(SchemaField::new(
                    field.name(),
                    Self::format_data_type_to_brewdb(field.data_type())?,
                )
                .with_nullable(field.data_type().is_nullable()))
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;
        Ok(TableSchema::new(columns))
    }

    fn brewdb_field_to_format_type(
        column: &SchemaField,
    ) -> Result<Self::FormatDataType, CatalogError> {
        let nullable = column.nullable;
        match column.data_type {
            DataType::Boolean => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(
                nullable,
            ))),
            DataType::Int8 => Ok(PaimonDataType::TinyInt(TinyIntType::with_nullable(
                nullable,
            ))),
            DataType::Int16 => Ok(PaimonDataType::SmallInt(SmallIntType::with_nullable(
                nullable,
            ))),
            DataType::Int32 => Ok(PaimonDataType::Int(IntType::with_nullable(nullable))),
            DataType::Int64 => Ok(PaimonDataType::BigInt(BigIntType::with_nullable(nullable))),
            DataType::Float32 => Ok(PaimonDataType::Float(FloatType::with_nullable(nullable))),
            DataType::Double => Ok(PaimonDataType::Double(DoubleType::with_nullable(nullable))),
            DataType::Binary => Ok(PaimonDataType::VarBinary(
                VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                    .map_err(map_paimon_backend_error)?,
            )),
            DataType::Date => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
            DataType::Time { precision } => Ok(PaimonDataType::Time(
                TimeType::with_nullable(nullable, precision).map_err(map_paimon_backend_error)?,
            )),
            DataType::Timestamp {
                precision,
                with_time_zone,
            } => {
                if with_time_zone {
                    Ok(PaimonDataType::LocalZonedTimestamp(
                        LocalZonedTimestampType::with_nullable(nullable, precision)
                            .map_err(map_paimon_backend_error)?,
                    ))
                } else {
                    Ok(PaimonDataType::Timestamp(
                        TimestampType::with_nullable(nullable, precision)
                            .map_err(map_paimon_backend_error)?,
                    ))
                }
            }
            DataType::Decimal { precision, scale } => Ok(PaimonDataType::Decimal(
                DecimalType::with_nullable(nullable, precision, scale)
                    .map_err(map_paimon_backend_error)?,
            )),
            DataType::String => Ok(PaimonDataType::VarChar(
                VarCharType::with_nullable(nullable, u32::MAX).map_err(map_paimon_backend_error)?,
            )),
        }
    }

    fn format_data_type_to_brewdb(
        data_type: &Self::FormatDataType,
    ) -> Result<DataType, CatalogError> {
        match data_type {
            PaimonDataType::Boolean(_) => Ok(DataType::Boolean),
            PaimonDataType::TinyInt(_) => Ok(DataType::Int8),
            PaimonDataType::SmallInt(_) => Ok(DataType::Int16),
            PaimonDataType::Int(_) => Ok(DataType::Int32),
            PaimonDataType::BigInt(_) => Ok(DataType::Int64),
            PaimonDataType::Float(_) => Ok(DataType::Float32),
            PaimonDataType::Double(_) => Ok(DataType::Double),
            PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) | PaimonDataType::Blob(_) => {
                Ok(DataType::Binary)
            }
            PaimonDataType::Date(_) => Ok(DataType::Date),
            PaimonDataType::Time(time) => Ok(DataType::Time {
                precision: time.precision(),
            }),
            PaimonDataType::Timestamp(timestamp) => Ok(DataType::Timestamp {
                precision: timestamp.precision(),
                with_time_zone: false,
            }),
            PaimonDataType::LocalZonedTimestamp(timestamp) => Ok(DataType::Timestamp {
                precision: timestamp.precision(),
                with_time_zone: true,
            }),
            PaimonDataType::Decimal(decimal) => Ok(DataType::Decimal {
                precision: decimal.precision(),
                scale: decimal.scale(),
            }),
            PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => Ok(DataType::String),
            other => Err(CatalogError::UnsupportedSchemaType {
                backend: "paimon",
                type_name: format!("{other:?}"),
            }),
        }
    }
}

fn map_paimon_backend_error(error: impl ToString) -> CatalogError {
    CatalogError::CatalogBackend {
        backend: "paimon",
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};
    use paimon::spec::{
        ArrayType, DataType as PaimonDataType, IntType, Schema, TableSchema as PaimonTableSchema,
    };

    use crate::errors::CatalogError;
    use crate::storage_format_schema::StorageFormatSchemaAdapter;

    use super::PaimonSchemaAdapter;

    #[test]
    fn brewdb_field_round_trips_with_paimon_type() {
        let field = SchemaField::new(
            "event_time",
            DataType::Timestamp {
                precision: 6,
                with_time_zone: true,
            },
        )
        .with_nullable(false);

        let paimon_type = PaimonSchemaAdapter::brewdb_field_to_format_type(&field).unwrap();
        let round_trip = SchemaField::new(
            "event_time",
            PaimonSchemaAdapter::format_data_type_to_brewdb(&paimon_type).unwrap(),
        )
        .with_nullable(paimon_type.is_nullable());

        assert_eq!(round_trip, field);
    }

    #[test]
    fn paimon_table_schema_maps_to_brewdb_table_schema() {
        let schema = Schema::builder()
            .column("id", PaimonDataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = PaimonTableSchema::new(1, &schema);

        let brewdb_schema = PaimonSchemaAdapter::table_schema_to_brewdb(&table_schema).unwrap();

        assert_eq!(
            brewdb_schema,
            TableSchema::new(vec![SchemaField::new("id", DataType::Int32)])
        );
    }

    #[test]
    fn unsupported_paimon_type_returns_catalog_error() {
        let error = PaimonSchemaAdapter::format_data_type_to_brewdb(&PaimonDataType::Array(
            ArrayType::new(PaimonDataType::Int(IntType::new())),
        ))
        .unwrap_err();

        assert!(matches!(
            error,
            CatalogError::UnsupportedSchemaType {
                backend: "paimon",
                ..
            }
        ));
    }
}
