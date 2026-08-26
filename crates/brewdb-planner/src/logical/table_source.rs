use crate::catalog::TableCatalogEntry;
use crate::common::table::TableSchema;
use crate::storage::{StorageError, TableEngine};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraint, Constraints};
use datafusion_expr::{TableSource, TableType};
use std::sync::Arc;

pub(crate) struct DefaultTableSource {
    table: TableCatalogEntry,
    table_engine: Option<Arc<dyn TableEngine>>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl std::fmt::Debug for DefaultTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTableSource")
            .field("table", &self.table)
            .field("has_table_engine", &self.table_engine.is_some())
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl Clone for DefaultTableSource {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            table_engine: self.table_engine.clone(),
            schema: Arc::clone(&self.schema),
            constraints: self.constraints.clone(),
        }
    }
}

impl DefaultTableSource {
    pub(crate) fn new(table: TableCatalogEntry) -> Self {
        let schema = table
            .table_schema
            .to_arrow_schema_ref()
            .expect("catalog table schema must be convertible to Arrow");
        let constraints = constraints_from_table_schema(&table.table_schema)
            .expect("catalog table primary keys must reference schema fields");
        Self {
            table,
            table_engine: None,
            schema,
            constraints,
        }
    }

    pub(crate) fn new_with_engine(
        table: TableCatalogEntry,
        table_engine: Arc<dyn TableEngine>,
    ) -> Result<Self, StorageError> {
        let schema = table_engine.schema_ref()?;
        let constraints = constraints_from_table_schema(&table.table_schema)
            .map_err(|reason| StorageError::TableScanFailed { reason })?;
        Ok(Self {
            table,
            table_engine: Some(table_engine),
            schema,
            constraints,
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

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

fn constraints_from_table_schema(schema: &TableSchema) -> Result<Constraints, String> {
    if schema.primary_keys.is_empty() {
        return Ok(Constraints::default());
    }

    let mut indices = Vec::with_capacity(schema.primary_keys.len());
    for primary_key in &schema.primary_keys {
        let Some(index) = schema
            .fields
            .iter()
            .position(|field| field.name == *primary_key)
        else {
            return Err(format!(
                "PRIMARY KEY column `{primary_key}` is not defined in table schema"
            ));
        };
        indices.push(index);
    }
    Ok(Constraints::new_unverified(vec![Constraint::PrimaryKey(
        indices,
    )]))
}
