//! Paimon <-> BrewDB schema conversion helpers kept local to the catalog module.

use std::collections::BTreeMap;

use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
use paimon::spec::{
    BigIntType, BooleanType, DataType as PaimonDataType, DateType, DecimalType, DoubleType,
    FloatType, IntType, LocalZonedTimestampType, Schema, SchemaChange, SmallIntType, TimeType,
    TimestampType, TinyIntType, VarBinaryType, VarCharType,
};

use crate::catalog::errors::CatalogError;
use crate::catalog::requests::{AlterTableOperation, CreateTableRequest};
use crate::catalog::storage_format_schema::StorageFormatSchemaAdapter;

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
        if !request.table_schema.primary_keys.is_empty() {
            builder = builder.primary_key(request.table_schema.primary_keys.iter().cloned());
        }
        if !request.table_schema.partition_keys.is_empty() {
            builder = builder.partition_keys(request.table_schema.partition_keys.iter().cloned());
        }
        for (key, value) in paimon_create_table_options(request)? {
            builder = builder.option(key, value);
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
                Self::brewdb_field_to_format_type(&ColumnField::new(
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
                Ok(ColumnField::new(
                    field.name(),
                    Self::format_data_type_to_brewdb(field.data_type())?,
                )
                .with_nullable(field.data_type().is_nullable()))
            })
            .collect::<Result<Vec<_>, CatalogError>>()?;
        let mut table_schema = TableSchema::new(columns)
            .with_primary_keys(schema.primary_keys().iter().cloned())
            .with_partition_keys(schema.partition_keys().iter().cloned())
            .with_bucket_keys(schema.bucket_keys())
            .with_cluster_keys(paimon_cluster_keys(schema));
        table_schema.bucket_count = schema
            .options()
            .get("bucket")
            .and_then(|value| value.parse::<u32>().ok());
        table_schema.bucket_function = schema.options().get("bucket-function.type").cloned();
        Ok(table_schema)
    }

    fn brewdb_field_to_format_type(
        column: &ColumnField,
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

fn paimon_create_table_options(
    request: &CreateTableRequest,
) -> Result<BTreeMap<String, String>, CatalogError> {
    let mut options = request.table_options.clone();
    if !request.table_schema.bucket_keys.is_empty() {
        options.insert(
            "bucket-key".to_string(),
            request.table_schema.bucket_keys.join(","),
        );
    } else if request.table_schema.bucket_function.is_some() {
        return Err(CatalogError::CatalogBackend {
            backend: "paimon",
            message: "bucket function requires at least one bucket key".to_string(),
        });
    }

    if let Some(function) = &request.table_schema.bucket_function {
        match function.to_ascii_lowercase().as_str() {
            "default" | "hash" => {
                options.remove("bucket-function.type");
            }
            "mod" => {
                options.insert("bucket-function.type".to_string(), "mod".to_string());
            }
            "hive" => {
                options.insert("bucket-function.type".to_string(), "hive".to_string());
            }
            other => {
                return Err(CatalogError::CatalogBackend {
                    backend: "paimon",
                    message: format!(
                        "unsupported bucket function `{other}`; supported functions are default, hash, mod, hive"
                    ),
                });
            }
        }
    }

    if let Some(bucket_count) = request.table_schema.bucket_count {
        options.insert("bucket".to_string(), bucket_count.to_string());
    }

    if !request.table_schema.cluster_keys.is_empty() {
        if !request.table_schema.primary_keys.is_empty() {
            return Err(CatalogError::CatalogBackend {
                backend: "paimon",
                message: "CLUSTER BY is only supported for append-only Paimon tables".to_string(),
            });
        }
        options.insert("clustering.incremental".to_string(), "true".to_string());
        options.insert(
            "clustering.columns".to_string(),
            request.table_schema.cluster_keys.join(","),
        );
        options.insert("clustering.strategy".to_string(), "order".to_string());
    }

    Ok(options)
}

fn paimon_cluster_keys(schema: &paimon::spec::TableSchema) -> Vec<String> {
    schema
        .options()
        .get("clustering.columns")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn map_paimon_backend_error(error: impl ToString) -> CatalogError {
    CatalogError::CatalogBackend {
        backend: "paimon",
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use paimon::spec::{
        ArrayType, DataType as PaimonDataType, IntType, Schema, TableSchema as PaimonTableSchema,
    };

    use crate::catalog::errors::CatalogError;
    use crate::catalog::requests::CreateTableRequest;
    use crate::catalog::storage_format_schema::StorageFormatSchemaAdapter;

    use super::PaimonSchemaAdapter;

    #[test]
    fn brewdb_field_round_trips_with_paimon_type() {
        let field = ColumnField::new(
            "event_time",
            DataType::Timestamp {
                precision: 6,
                with_time_zone: true,
            },
        )
        .with_nullable(false);

        let paimon_type = PaimonSchemaAdapter::brewdb_field_to_format_type(&field).unwrap();
        let round_trip = ColumnField::new(
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
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)])
                .with_bucket_keys(["id"])
        );
    }

    #[test]
    fn paimon_create_schema_maps_brewdb_layout_to_paimon_options() {
        let schema = PaimonSchemaAdapter::build_schema(
            &CreateTableRequest::new(
                "sales",
                "orders",
                TableSchema::new(vec![
                    ColumnField::new("id", DataType::Int32),
                    ColumnField::new("dt", DataType::String),
                ])
                .with_partition_keys(["dt"])
                .with_bucket_keys(["id"])
                .with_bucket_count(8)
                .with_bucket_function("mod")
                .with_cluster_keys(["dt"]),
            )
            .with_options([("write-buffer-size", "64mb")]),
        )
        .unwrap();

        assert_eq!(schema.partition_keys(), &vec!["dt".to_owned()]);
        assert_eq!(
            schema.options().get("bucket-key").map(String::as_str),
            Some("id")
        );
        assert_eq!(
            schema.options().get("bucket").map(String::as_str),
            Some("8")
        );
        assert_eq!(
            schema
                .options()
                .get("bucket-function.type")
                .map(String::as_str),
            Some("mod")
        );
        assert_eq!(
            schema
                .options()
                .get("clustering.columns")
                .map(String::as_str),
            Some("dt")
        );
        assert_eq!(
            schema
                .options()
                .get("write-buffer-size")
                .map(String::as_str),
            Some("64mb")
        );
    }

    #[test]
    fn paimon_create_schema_rejects_cluster_by_primary_key_tables() {
        let error = PaimonSchemaAdapter::build_schema(&CreateTableRequest::new(
            "sales",
            "orders",
            TableSchema::new(vec![
                ColumnField::new("id", DataType::Int32),
                ColumnField::new("dt", DataType::String),
            ])
            .with_primary_keys(["id"])
            .with_cluster_keys(["dt"]),
        ))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "catalog backend `paimon` error: CLUSTER BY is only supported for append-only Paimon tables"
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
