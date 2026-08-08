//! BrewDB storage contracts.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, RwLock};

use arrow::record_batch::RecordBatch;
use brewdb_catalog::TableCatalogEntry;
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;

pub use inventory;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageError {
    StorageRegistryInvalid { reason: String },
    TableNotFound { table_id: uuid::Uuid },
    UnsupportedTableFormat { format: String },
    TableScanFailed { reason: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageRegistryInvalid { reason } => {
                write!(f, "storage registry invalid: {reason}")
            }
            Self::TableNotFound { table_id } => write!(f, "table not found: {table_id}"),
            Self::UnsupportedTableFormat { format } => {
                write!(f, "unsupported table format: {format}")
            }
            Self::TableScanFailed { reason } => write!(f, "table scan failed: {reason}"),
        }
    }
}

impl Error for StorageError {}

pub trait TableEngine: Send + Sync {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError>;
}

pub trait StorageEngine: Send + Sync {
    fn table_engine(&self, table: &TableCatalogEntry)
    -> Result<Arc<dyn TableEngine>, StorageError>;
}

pub struct StorageEngineRegistration {
    pub lake_format: &'static str,
    pub open: fn() -> Arc<dyn StorageEngine>,
}

inventory::collect!(StorageEngineRegistration);

#[macro_export]
macro_rules! register_storage_engine {
    ($lake_format:expr, $open:expr) => {
        $crate::inventory::submit! {
            $crate::StorageEngineRegistration {
                lake_format: $lake_format,
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
        let format = table.lake_format_kind.as_str();
        let engine =
            self.engines
                .get(format)
                .ok_or_else(|| StorageError::UnsupportedTableFormat {
                    format: format.to_owned(),
                })?;
        engine.table_engine(table)
    }
}

pub fn open_storage_engine() -> Result<Arc<dyn StorageEngine>, StorageError> {
    let mut engines = BTreeMap::new();
    for registration in inventory::iter::<StorageEngineRegistration> {
        if engines
            .insert(registration.lake_format, (registration.open)())
            .is_some()
        {
            return Err(StorageError::StorageRegistryInvalid {
                reason: format!(
                    "duplicate storage engine registration for `{}`",
                    registration.lake_format
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
            .ok_or(StorageError::TableNotFound {
                table_id: table.table_id,
            })
    }
}

#[cfg(test)]
mod tests {
    use brewdb_catalog::{CatalogMode, LakeFormatKind, TableCatalogEntry, TablePath};
    use brewdb_common::schema::{DataType, SchemaField, TableSchema};

    use super::{MemoryStorageEngine, StorageEngine, StorageError};

    fn make_table() -> TableCatalogEntry {
        TableCatalogEntry::new(
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            TablePath::new("prod", "sales", "orders").unwrap(),
            TableSchema::new(vec![SchemaField::new("id", DataType::Int32)]),
            "s3://warehouse/sales/orders",
            LakeFormatKind::Paimon,
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
    fn lake_format_kind_string_mapping_is_stable() {
        assert_eq!(LakeFormatKind::Paimon.as_str(), "paimon");
        assert_eq!(LakeFormatKind::Iceberg.as_str(), "iceberg");
    }
}
