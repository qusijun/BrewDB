use crate::catalog::TableCatalogEntry;
use crate::storage::TableEngine;
use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraints, Result as DataFusionResult};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableSource, TableType};
use std::sync::Arc;

pub(crate) struct DefaultTableSource {
    table: TableCatalogEntry,
    table_engine: Arc<dyn TableEngine>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl std::fmt::Debug for DefaultTableSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultTableSource")
            .field("table", &self.table)
            .field("storage_kind", &self.table_engine.storage_kind())
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl Clone for DefaultTableSource {
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            table_engine: Arc::clone(&self.table_engine),
            schema: Arc::clone(&self.schema),
            constraints: self.constraints.clone(),
        }
    }
}

impl DefaultTableSource {
    pub(crate) fn new(table: TableCatalogEntry, table_engine: Arc<dyn TableEngine>) -> Self {
        // Logical planning should use the catalog schema as the source of
        // truth. The engine is attached here only so DataFusion optimizer
        // rules can query storage capabilities such as filter pushdown.
        let schema = table
            .table_schema
            .to_arrow_schema_ref()
            .expect("catalog table schema must be convertible to Arrow");
        let constraints = table
            .table_schema
            .datafusion_constraints()
            .expect("catalog table primary keys must reference schema fields");
        Self {
            table,
            table_engine,
            schema,
            constraints,
        }
    }

    pub(crate) fn table(&self) -> &TableCatalogEntry {
        &self.table
    }

    pub(crate) fn table_engine(&self) -> &Arc<dyn TableEngine> {
        &self.table_engine
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.table_engine.supports_filters_pushdown(filters)
    }
}
