//! Shared table metadata helpers.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::{Constraint, Constraints, TableReference};

use crate::common::{column::ColumnField, errors::CommonError};

const PRIMARY_KEYS_METADATA_KEY: &str = "brewdb.table.primary_keys";
const PARTITION_KEYS_METADATA_KEY: &str = "brewdb.table.partition_keys";
const BUCKET_KEYS_METADATA_KEY: &str = "brewdb.table.bucket_keys";
const BUCKET_COUNT_METADATA_KEY: &str = "brewdb.table.bucket_count";
const BUCKET_FUNCTION_METADATA_KEY: &str = "brewdb.table.bucket_function";
const CLUSTER_KEYS_METADATA_KEY: &str = "brewdb.table.cluster_keys";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub fields: Vec<ColumnField>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub bucket_count: Option<u32>,
    pub bucket_function: Option<String>,
    pub cluster_keys: Vec<String>,
}

impl TableSchema {
    pub fn new(fields: Vec<ColumnField>) -> Self {
        Self {
            fields,
            primary_keys: Vec::new(),
            partition_keys: Vec::new(),
            bucket_keys: Vec::new(),
            bucket_count: None,
            bucket_function: None,
            cluster_keys: Vec::new(),
        }
    }

    pub fn with_primary_keys(
        mut self,
        primary_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.primary_keys = primary_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_partition_keys(
        mut self,
        partition_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_keys = partition_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_bucket_keys(
        mut self,
        bucket_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.bucket_keys = bucket_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_bucket_count(mut self, bucket_count: u32) -> Self {
        self.bucket_count = Some(bucket_count);
        self
    }

    pub fn with_bucket_function(mut self, bucket_function: impl Into<String>) -> Self {
        self.bucket_function = Some(bucket_function.into());
        self
    }

    pub fn with_cluster_keys(
        mut self,
        cluster_keys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.cluster_keys = cluster_keys.into_iter().map(Into::into).collect();
        self
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, CommonError> {
        let mut metadata = std::collections::HashMap::new();
        insert_list_metadata(&mut metadata, PRIMARY_KEYS_METADATA_KEY, &self.primary_keys);
        insert_list_metadata(
            &mut metadata,
            PARTITION_KEYS_METADATA_KEY,
            &self.partition_keys,
        );
        insert_list_metadata(&mut metadata, BUCKET_KEYS_METADATA_KEY, &self.bucket_keys);
        if let Some(bucket_count) = self.bucket_count {
            metadata.insert(
                BUCKET_COUNT_METADATA_KEY.to_owned(),
                bucket_count.to_string(),
            );
        }
        if let Some(bucket_function) = &self.bucket_function {
            metadata.insert(
                BUCKET_FUNCTION_METADATA_KEY.to_owned(),
                bucket_function.clone(),
            );
        }
        insert_list_metadata(&mut metadata, CLUSTER_KEYS_METADATA_KEY, &self.cluster_keys);
        Ok(ArrowSchema::new_with_metadata(
            self.fields
                .iter()
                .map(ColumnField::to_arrow_field)
                .collect::<Result<Vec<_>, _>>()?,
            metadata,
        ))
    }

    pub fn to_arrow_schema_ref(&self) -> Result<Arc<ArrowSchema>, CommonError> {
        Ok(Arc::new(self.to_arrow_schema()?))
    }

    pub fn from_arrow_schema(schema: &ArrowSchema) -> Result<Self, CommonError> {
        let metadata = schema.metadata();
        let bucket_count = metadata
            .get(BUCKET_COUNT_METADATA_KEY)
            .and_then(|value| value.parse::<u32>().ok());
        let bucket_function = metadata.get(BUCKET_FUNCTION_METADATA_KEY).cloned();
        Ok(Self {
            fields: schema
                .fields()
                .iter()
                .map(|field| ColumnField::from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
            primary_keys: metadata_list(metadata, PRIMARY_KEYS_METADATA_KEY),
            partition_keys: metadata_list(metadata, PARTITION_KEYS_METADATA_KEY),
            bucket_keys: metadata_list(metadata, BUCKET_KEYS_METADATA_KEY),
            bucket_count,
            bucket_function,
            cluster_keys: metadata_list(metadata, CLUSTER_KEYS_METADATA_KEY),
        })
    }
}

fn insert_list_metadata(
    metadata: &mut std::collections::HashMap<String, String>,
    key: &str,
    values: &[String],
) {
    if !values.is_empty() {
        metadata.insert(key.to_owned(), values.join(","));
    }
}

fn metadata_list(metadata: &std::collections::HashMap<String, String>, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableReferenceParts {
    pub catalog_name: String,
    pub database_name: String,
    pub table_name: String,
}

pub fn table_reference_parts(
    reference: &TableReference,
) -> Result<TableReferenceParts, CommonError> {
    match reference {
        TableReference::Full {
            catalog,
            schema,
            table,
        } => Ok(TableReferenceParts {
            catalog_name: catalog.to_string(),
            database_name: schema.to_string(),
            table_name: table.to_string(),
        }),
        _ => Err(CommonError::InvalidTableReference {
            reference: reference.to_string(),
        }),
    }
}

pub fn primary_key_names(schema: &ArrowSchema, constraints: &Constraints) -> Vec<String> {
    constraints
        .iter()
        .find_map(|constraint| match constraint {
            Constraint::PrimaryKey(indices) => Some(
                indices
                    .iter()
                    .filter_map(|index| schema.fields().get(*index))
                    .map(|field| field.name().clone())
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
    };

    use crate::common::{column::ColumnField, datatype::DataType};

    use super::*;

    #[test]
    fn brewdb_schema_round_trips_through_arrow_schema() {
        let schema = TableSchema::new(vec![
            ColumnField::new("id", DataType::Int64).with_nullable(false),
            ColumnField::new(
                "event_time",
                DataType::Timestamp {
                    precision: 6,
                    with_time_zone: true,
                },
            ),
            ColumnField::new(
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
    fn table_schema_carries_table_layout_keys() {
        let schema = TableSchema::new(vec![
            ColumnField::new("id", DataType::Int64),
            ColumnField::new("dt", DataType::String),
            ColumnField::new("region", DataType::String),
        ])
        .with_primary_keys(["id"])
        .with_partition_keys(["dt"])
        .with_bucket_keys(["id"])
        .with_bucket_count(8)
        .with_bucket_function("hash")
        .with_cluster_keys(["region"]);

        assert_eq!(schema.primary_keys, vec!["id"]);
        assert_eq!(schema.partition_keys, vec!["dt"]);
        assert_eq!(schema.bucket_keys, vec!["id"]);
        assert_eq!(schema.bucket_count, Some(8));
        assert_eq!(schema.bucket_function.as_deref(), Some("hash"));
        assert_eq!(schema.cluster_keys, vec!["region"]);
    }

    #[test]
    fn table_schema_layout_keys_round_trip_through_arrow_metadata() {
        let schema = TableSchema::new(vec![
            ColumnField::new("id", DataType::Int64),
            ColumnField::new("dt", DataType::String),
        ])
        .with_primary_keys(["id"])
        .with_partition_keys(["dt"])
        .with_bucket_keys(["id"])
        .with_bucket_count(4)
        .with_bucket_function("mod")
        .with_cluster_keys(["dt"]);

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

    #[test]
    fn table_reference_parts_extracts_fully_qualified_table_name() {
        let parts =
            table_reference_parts(&TableReference::full("prod", "sales", "orders")).unwrap();

        assert_eq!(
            parts,
            TableReferenceParts {
                catalog_name: "prod".to_owned(),
                database_name: "sales".to_owned(),
                table_name: "orders".to_owned(),
            }
        );
    }

    #[test]
    fn table_reference_parts_rejects_partial_table_name() {
        let error = table_reference_parts(&TableReference::bare("orders")).unwrap_err();

        assert_eq!(
            error.to_string(),
            "table reference must be fully qualified: orders"
        );
    }

    #[test]
    fn primary_key_names_extracts_fields_from_primary_key_constraint() {
        let schema = ArrowSchema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("payload", arrow::datatypes::DataType::Utf8, true),
        ]);
        let constraints = Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])]);

        assert_eq!(
            primary_key_names(&schema, &constraints),
            vec!["id".to_owned(), "region".to_owned()]
        );
    }
}
