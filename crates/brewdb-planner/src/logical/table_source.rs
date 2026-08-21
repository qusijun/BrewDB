use crate::catalog::TableCatalogEntry;
use crate::storage::{StorageError, TableEngine};
use arrow::datatypes::SchemaRef;
use datafusion_expr::{TableSource, TableType};
use std::sync::Arc;

pub(crate) struct DefaultTableSource {
    table: TableCatalogEntry,
    table_engine: Option<Arc<dyn TableEngine>>,
    schema: SchemaRef,
}

impl std::fmt::Debug for DefaultTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTableSource")
            .field("table", &self.table)
            .field("has_table_engine", &self.table_engine.is_some())
            .field("schema", &self.schema)
            .finish()
    }
}

impl Clone for DefaultTableSource {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            table_engine: self.table_engine.clone(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl DefaultTableSource {
    pub(crate) fn new(table: TableCatalogEntry) -> Self {
        let schema = table
            .table_schema
            .to_arrow_schema_ref()
            .expect("catalog table schema must be convertible to Arrow");
        Self {
            table,
            table_engine: None,
            schema,
        }
    }

    pub(crate) fn new_with_engine(
        table: TableCatalogEntry,
        table_engine: Arc<dyn TableEngine>,
    ) -> Result<Self, StorageError> {
        let schema = table_engine.schema_ref()?;
        Ok(Self {
            table,
            table_engine: Some(table_engine),
            schema,
        })
    }

    pub(crate) fn table(&self) -> &TableCatalogEntry {
        &self.table
    }

    pub(crate) fn table_engine(&self) -> Option<&Arc<dyn TableEngine>> {
        self.table_engine.as_ref()
    }
}

impl TableSource for DefaultTableSource {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
