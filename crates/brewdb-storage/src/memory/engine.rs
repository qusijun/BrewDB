use std::sync::Arc;

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::storage::{StorageError, TableEngine, TableEngineFactory};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};

pub struct MemoryTableEngine {
    provider: Arc<dyn TableProvider>,
}

impl MemoryTableEngine {
    pub fn new(provider: Arc<dyn TableProvider>) -> Self {
        Self { provider }
    }

    pub fn try_new(
        table: &TableCatalogEntry,
        batches: Vec<Vec<RecordBatch>>,
    ) -> Result<Self, StorageError> {
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
        Ok(Self::new(provider))
    }
}

impl TableEngine for MemoryTableEngine {
    fn storage_kind(&self) -> StorageKind {
        StorageKind::Memory
    }

    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        Ok(Arc::clone(&self.provider))
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryTableEngineFactory;

impl TableEngineFactory for MemoryTableEngineFactory {
    fn create_table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.storage_kind != StorageKind::Memory {
            return Err(StorageError::UnsupportedStorageKind {
                storage_kind: table.storage_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(MemoryTableEngine::try_new(table, vec![vec![]])?))
    }
}

fn open_memory_table_engine_factory() -> Arc<dyn TableEngineFactory> {
    Arc::new(MemoryTableEngineFactory)
}

crate::register_table_engine_factory!(StorageKind::Memory, open_memory_table_engine_factory);

#[cfg(test)]
mod tests {
    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry, TablePath};
    use crate::common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use crate::storage::{StorageError, open_storage_engine};

    fn make_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            StorageKind::Iceberg,
            CatalogMode::Managed,
        )
    }

    fn make_memory_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "scratch").unwrap(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            "memory://scratch",
            StorageKind::Memory,
            CatalogMode::Temporary,
        )
    }

    #[test]
    fn storage_engine_rejects_unsupported_storage_kind() {
        let storage = open_storage_engine().unwrap();
        assert!(matches!(
            storage.table_engine(&make_table()),
            Err(StorageError::UnsupportedStorageKind { .. })
        ));
    }

    #[test]
    fn storage_engine_opens_registered_memory_table_engine() {
        let storage = open_storage_engine().unwrap();
        let table = make_memory_table();
        let engine = storage.table_engine(&table).unwrap();

        assert_eq!(engine.schema_ref().unwrap().field(0).name(), "id");
    }
}
