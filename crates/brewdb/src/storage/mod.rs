//! BrewDB storage contracts.

pub mod file;
pub mod paimon;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::planner::distributed::split::{TableScanSplit, TableScanSplitGroup};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion_expr::TableScan;

pub use inventory;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    StorageRegistryInvalid { reason: String },
    TableNotFound { table_id: uuid::Uuid },
    UnsupportedStorageKind { storage_kind: String },
    TableScanFailed { reason: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageRegistryInvalid { reason } => {
                write!(f, "storage registry invalid: {reason}")
            }
            Self::TableNotFound { table_id } => write!(f, "table not found: {table_id}"),
            Self::UnsupportedStorageKind { storage_kind } => {
                write!(f, "unsupported storage kind: {storage_kind}")
            }
            Self::TableScanFailed { reason } => write!(f, "table scan failed: {reason}"),
        }
    }
}

impl Error for StorageError {}

pub trait TableEngine: Send + Sync {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError>;

    fn schema_ref(&self) -> Result<SchemaRef, StorageError> {
        Ok(self.table_provider()?.schema())
    }

    fn get_table_provider(
        &self,
        _splits: &TableScanSplitGroup,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider()
    }

    fn plan_scan(&self, scan: &TableScan) -> Result<TableScanSplitGroup, StorageError> {
        Ok(TableScanSplitGroup::new(vec![TableScanSplit::new(
            scan.table_name.to_string(),
            0,
        )]))
    }
}

pub trait StorageEngine: Send + Sync {
    fn table_engine(&self, table: &TableCatalogEntry)
        -> Result<Arc<dyn TableEngine>, StorageError>;
}

pub struct StorageEngineRegistration {
    pub storage_kind: &'static str,
    pub open: fn() -> Arc<dyn StorageEngine>,
}

inventory::collect!(StorageEngineRegistration);

#[macro_export]
macro_rules! register_storage_engine {
    ($storage_kind:expr, $open:expr) => {
        $crate::storage::inventory::submit! {
            $crate::storage::StorageEngineRegistration {
                storage_kind: $storage_kind,
                open: $open,
            }
        }
    };
}

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

struct RegistryStorageEngine {
    engines: BTreeMap<&'static str, Arc<dyn StorageEngine>>,
}

impl StorageEngine for RegistryStorageEngine {
    fn table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        let storage_kind = table.storage_kind.as_str();
        let engine =
            self.engines
                .get(storage_kind)
                .ok_or_else(|| StorageError::UnsupportedStorageKind {
                    storage_kind: storage_kind.to_owned(),
                })?;
        engine.table_engine(table)
    }
}

pub fn open_storage_engine() -> Result<Arc<dyn StorageEngine>, StorageError> {
    let mut engines = BTreeMap::new();
    for registration in inventory::iter::<StorageEngineRegistration> {
        if engines
            .insert(registration.storage_kind, (registration.open)())
            .is_some()
        {
            return Err(StorageError::StorageRegistryInvalid {
                reason: format!(
                    "duplicate storage engine registration for `{}`",
                    registration.storage_kind
                ),
            });
        }
    }
    Ok(Arc::new(RegistryStorageEngine { engines }))
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

    use super::{open_storage_engine, MemoryStorageEngine, StorageEngine, StorageError};

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

    #[test]
    fn storage_kind_string_mapping_is_stable() {
        assert_eq!(StorageKind::Paimon.as_str(), "paimon");
        assert_eq!(StorageKind::Iceberg.as_str(), "iceberg");
        assert_eq!(StorageKind::File.as_str(), "file");
    }

    #[test]
    fn registry_storage_opens_file_table_engine_from_temporary_file_entry() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = std::env::temp_dir()
                .join(format!("brewdb-file-entry-{}.csv", uuid::Uuid::new_v4()));
            std::fs::write(&path, "id\n7\n").unwrap();
            let table = TableCatalogEntry::temporary_file(
                "copy_source",
                path.to_string_lossy().to_string(),
                [("format", "csv"), ("has_header", "true")],
            )
            .unwrap();

            let storage = open_storage_engine().unwrap();
            let provider = storage
                .table_engine(&table)
                .unwrap()
                .table_provider()
                .unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(table.storage_kind, StorageKind::File);
            assert_eq!(table.catalog_mode, CatalogMode::Temporary);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].schema().field(0).name(), "id");
        });
    }
}
