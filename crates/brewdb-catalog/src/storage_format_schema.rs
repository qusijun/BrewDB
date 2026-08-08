//! Storage-format-local schema conversion interfaces.

use brewdb_common::schema::{DataType, SchemaField, TableSchema};

use crate::errors::CatalogError;
use crate::requests::{AlterTableOperation, CreateTableRequest};

pub trait StorageFormatSchemaAdapter {
    type FormatDataType;
    type CreateSchema;
    type FormatTableSchema;
    type AlterChange;

    fn build_schema(request: &CreateTableRequest) -> Result<Self::CreateSchema, CatalogError>;

    fn alter_operation_to_change(
        operation: &AlterTableOperation,
    ) -> Result<Self::AlterChange, CatalogError>;

    fn table_schema_to_brewdb(
        schema: &Self::FormatTableSchema,
    ) -> Result<TableSchema, CatalogError>;

    fn brewdb_field_to_format_type(
        column: &SchemaField,
    ) -> Result<Self::FormatDataType, CatalogError>;

    fn format_data_type_to_brewdb(
        data_type: &Self::FormatDataType,
    ) -> Result<DataType, CatalogError>;
}
