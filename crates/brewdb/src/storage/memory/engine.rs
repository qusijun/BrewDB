use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::storage::{open_storage_engine, StorageEngine, StorageError, TableEngine};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};

pub struct MemoryTableEngine {
    provider: Arc<dyn TableProvider>,
}

impl MemoryTableEngine {
    pub fn new(provider: Arc<dyn TableProvider>) -> Self {
        Self { provider }
    }
}

impl TableEngine for MemoryTableEngine {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        Ok(Arc::clone(&self.provider))
    }
}

#[derive(Default)]
pub struct MemoryStorageEngine {
    tables: RwLock<BTreeMap<uuid::Uuid, Arc<dyn TableEngine>>>,
}

impl MemoryStorageEngine {
    pub fn register_table_engine(&self, table: &TableCatalogEntry, engine: Arc<dyn TableEngine>) {
        self.tables
            .write()
            .expect("storage lock must not be poisoned")
            .insert(table.table_id, engine);
    }

    pub fn register_table_provider(
        &self,
        table: &TableCatalogEntry,
        provider: Arc<dyn TableProvider>,
    ) {
        self.register_table_engine(table, Arc::new(MemoryTableEngine::new(provider)));
    }

    pub fn register_batches(
        &self,
        table: &TableCatalogEntry,
        batches: Vec<Vec<RecordBatch>>,
    ) -> Result<(), StorageError> {
        let provider = Arc::new(
            MemTable::try_new(
                table.table_schema.to_arrow_schema_ref().map_err(|err| {
                    StorageError::TableScanFailed {
                        reason: err.to_string(),
                    }
                })?,
                batches,
            )
            .map_err(|err| StorageError::TableScanFailed {
                reason: err.to_string(),
            })?,
        );
        self.register_table_provider(table, provider);
        Ok(())
    }
}

impl StorageEngine for MemoryStorageEngine {
    fn table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        self.tables
            .read()
            .expect("storage lock must not be poisoned")
            .get(&table.table_id)
            .cloned()
            .map(Ok)
            .unwrap_or_else(|| {
                if table.storage_kind == StorageKind::File {
                    return open_storage_engine()?.table_engine(table);
                }
                Err(StorageError::TableNotFound {
                    table_id: table.table_id,
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry, TablePath};
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::storage::{StorageEngine, StorageError};

    use super::MemoryStorageEngine;

    fn make_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            StorageKind::Paimon,
            CatalogMode::Managed,
        )
    }

    #[test]
    fn memory_storage_rejects_missing_table() {
        let storage = MemoryStorageEngine::default();
        assert!(matches!(
            storage.table_engine(&make_table()),
            Err(StorageError::TableNotFound { .. })
        ));
    }
}
