//! Shared table metadata helpers.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion_common::{Constraint, Constraints, TableReference};

use crate::{column::ColumnField, errors::CommonError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub fields: Vec<ColumnField>,
}

impl TableSchema {
    pub fn new(fields: Vec<ColumnField>) -> Self {
        Self { fields }
    }

    pub fn to_arrow_schema(&self) -> Result<ArrowSchema, CommonError> {
        Ok(ArrowSchema::new(
            self.fields
                .iter()
                .map(ColumnField::to_arrow_field)
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
                .map(|field| ColumnField::from_arrow_field(field.as_ref()))
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }
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

    use crate::{column::ColumnField, datatype::DataType};

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
