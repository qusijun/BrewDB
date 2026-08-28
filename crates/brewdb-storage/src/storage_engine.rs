//! Process-level storage registry.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion_common::Result as DataFusionResult;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableScan};

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::storage::{StorageError, TableScanSplit, TableScanSplitGroup};

pub trait TableEngine: Send + Sync {
    fn storage_kind(&self) -> StorageKind;

    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError>;

    fn schema_ref(&self) -> Result<SchemaRef, StorageError> {
        Ok(self.table_provider()?.schema())
    }

    /// Builds the table provider used by worker-side physical planning.
    ///
    /// The optional split is the scan assignment chosen by coordinator-side
    /// planning. Implementations should use it to restrict the provider to the
    /// worker's assigned data before DataFusion builds the local physical scan.
    /// Coarse-grained pruning and split generation belong to [`Self::plan_scan`].
    fn get_table_provider(
        &self,
        _split: Option<&TableScanSplit>,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider()
    }

    /// Returns this engine's logical filter pushdown capability.
    ///
    /// This method is called from DataFusion's logical optimizer through
    /// BrewDB's `TableSource`. It must only classify filters as exact,
    /// inexact, or unsupported. It should not plan splits or perform pruning
    /// I/O; coarse-grained pruning happens later in [`Self::plan_scan`].
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    /// Plans a logical table scan into schedulable scan splits.
    ///
    /// This is the coordinator-side scan planning boundary. Implementations
    /// should use the projection, pushed filters, and fetch limit carried by
    /// the DataFusion `TableScan` to perform coarse-grained pruning before
    /// producing BrewDB scan splits. Examples include partition pruning,
    /// manifest pruning, file pruning, and storage-specific split planning.
    ///
    /// Worker-side scan execution may still apply finer-grained pruning inside
    /// an assigned split, such as row-group or page pruning.
    fn plan_scan(&self, scan: &TableScan) -> Result<TableScanSplitGroup, StorageError> {
        Ok(TableScanSplitGroup::new(vec![TableScanSplit::new(
            scan.table_name.to_string(),
            0,
        )]))
    }
}

pub trait TableEngineFactory: Send + Sync {
    fn create_table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError>;
}

pub struct StorageEngineRegistration {
    pub storage_kind: StorageKind,
    pub open: fn() -> Arc<dyn TableEngineFactory>,
}

inventory::collect!(StorageEngineRegistration);

#[macro_export]
macro_rules! register_table_engine_factory {
    ($storage_kind:expr, $open:expr) => {
        $crate::storage::inventory::submit! {
            $crate::storage::StorageEngineRegistration {
                storage_kind: $storage_kind,
                open: $open,
            }
        }
    };
}

pub struct StorageEngine {
    factories: BTreeMap<StorageKind, Arc<dyn TableEngineFactory>>,
    table_engines: RwLock<BTreeMap<uuid::Uuid, Arc<dyn TableEngine>>>,
}

impl StorageEngine {
    pub fn new(
        factories: impl IntoIterator<Item = (StorageKind, Arc<dyn TableEngineFactory>)>,
    ) -> Result<Self, StorageError> {
        let mut registry = BTreeMap::new();
        for (storage_kind, factory) in factories {
            if registry.insert(storage_kind, factory).is_some() {
                return Err(StorageError::StorageRegistryInvalid {
                    reason: format!(
                        "duplicate storage engine registration for `{}`",
                        storage_kind.as_str()
                    ),
                });
            }
        }
        Ok(Self {
            factories: registry,
            table_engines: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if let Some(engine) = self
            .table_engines
            .read()
            .expect("storage lock must not be poisoned")
            .get(&table.table_id)
            .cloned()
        {
            return Ok(engine);
        }

        let factory = self.factories.get(&table.storage_kind).ok_or_else(|| {
            StorageError::UnsupportedStorageKind {
                storage_kind: table.storage_kind.as_str().to_owned(),
            }
        })?;
        factory.create_table_engine(table)
    }

    pub fn register_table_engine(&self, table: &TableCatalogEntry, engine: Arc<dyn TableEngine>) {
        self.table_engines
            .write()
            .expect("storage lock must not be poisoned")
            .insert(table.table_id, engine);
    }
}

pub fn open_storage_engine() -> Result<Arc<StorageEngine>, StorageError> {
    StorageEngine::new(
        inventory::iter::<StorageEngineRegistration>
            .into_iter()
            .map(|registration| (registration.storage_kind, (registration.open)())),
    )
    .map(Arc::new)
}

#[cfg(test)]
mod tests {
    use crate::catalog::{CatalogMode, StorageKind, TableCatalogEntry};
    use crate::storage::open_storage_engine;
    use brewdb_common::test_util::TestFile;

    #[test]
    fn registry_storage_opens_file_table_engine_from_temporary_file_entry() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = TestFile::new("brewdb-file-entry", "csv");
            std::fs::write(path.path(), "id\n7\n").unwrap();
            let table = TableCatalogEntry::temporary_file(
                "copy_source",
                path.path().to_string_lossy().to_string(),
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

    #[test]
    fn table_engine_reports_its_storage_kind() {
        let storage = open_storage_engine().unwrap();
        let path = TestFile::new("brewdb-file-engine-kind", "csv");
        std::fs::write(path.path(), "id\n7\n").unwrap();
        let table = TableCatalogEntry::temporary_file(
            "copy_source",
            path.path().to_string_lossy().to_string(),
            [("format", "csv"), ("has_header", "true")],
        )
        .unwrap();

        let engine = storage.table_engine(&table).unwrap();

        assert_eq!(engine.storage_kind(), StorageKind::File);
    }
}
