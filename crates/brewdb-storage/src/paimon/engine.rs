use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use crate::catalog::{
    CreateTableRequest, PaimonSchemaAdapter, StorageFormatSchemaAdapter, StorageKind,
    TableCatalogEntry,
};
use crate::storage::{
    StorageError, TableEngine, TableEngineFactory, TableScanSplit, TableScanSplitGroup,
};
use datafusion::datasource::TableProvider;
use datafusion_expr::TableScan;
use paimon::DataSplit;
use paimon::catalog::Identifier as PaimonIdentifier;
use paimon::io::FileIO;
use paimon::spec::TableSchema as PaimonTableSchema;
use paimon::table::Table as PaimonTable;

use super::table_provider::PaimonTableProvider;

static PAIMON_SCAN_PLANS: OnceLock<Mutex<HashMap<String, Vec<DataSplit>>>> = OnceLock::new();

pub struct PaimonTableEngine {
    table: TableCatalogEntry,
}

#[derive(Default)]
pub struct PaimonTableEngineFactory;

impl TableEngineFactory for PaimonTableEngineFactory {
    fn create_table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.storage_kind != StorageKind::Paimon {
            return Err(StorageError::UnsupportedStorageKind {
                storage_kind: table.storage_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(PaimonTableEngine::new(table.clone())))
    }
}

fn open_paimon_table_engine_factory() -> Arc<dyn TableEngineFactory> {
    Arc::new(PaimonTableEngineFactory)
}

crate::register_table_engine_factory!(StorageKind::Paimon, open_paimon_table_engine_factory);

impl PaimonTableEngine {
    pub fn new(table: TableCatalogEntry) -> Self {
        Self { table }
    }

    fn build_table(&self) -> Result<PaimonTable, StorageError> {
        let file_io = FileIO::from_path(&self.table.table_location)
            .map_err(storage_scan_error)?
            .build()
            .map_err(storage_scan_error)?;
        let schema = build_paimon_schema(&self.table)?;
        let identifier = PaimonIdentifier::new(self.table.path.database(), self.table.path.table());
        Ok(PaimonTable::new(
            file_io,
            identifier,
            self.table.table_location.clone(),
            schema,
            None,
        ))
    }

    fn store_planned_splits(&self, splits: Vec<DataSplit>) -> String {
        let plan_key = uuid::Uuid::new_v4().to_string();
        PAIMON_SCAN_PLANS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("paimon scan plan registry must not be poisoned")
            .insert(plan_key.clone(), splits);
        plan_key
    }

    fn take_planned_splits(plan_key: &str, ordinals: &[u32]) -> Option<Vec<DataSplit>> {
        PAIMON_SCAN_PLANS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("paimon scan plan registry must not be poisoned")
            .remove(plan_key)
            .map(|splits| assigned_paimon_splits(splits, ordinals))
    }
}

impl TableEngine for PaimonTableEngine {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        Ok(Arc::new(PaimonTableProvider::try_new(
            self.build_table()?,
            None,
        )?))
    }

    fn get_table_provider(
        &self,
        splits: &TableScanSplitGroup,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        let plan_key = splits
            .splits
            .iter()
            .find_map(|split| split.properties.get("paimon.plan_key"))
            .cloned();
        let ordinals = splits
            .splits
            .iter()
            .filter_map(|split| {
                split
                    .properties
                    .contains_key("paimon.plan_key")
                    .then_some(split.ordinal)
            })
            .collect::<Vec<_>>();
        let planned_splits = plan_key
            .as_deref()
            .and_then(|plan_key| Self::take_planned_splits(plan_key, &ordinals));
        Ok(Arc::new(PaimonTableProvider::try_new(
            self.build_table()?,
            planned_splits,
        )?))
    }

    fn plan_scan(&self, scan: &TableScan) -> Result<TableScanSplitGroup, StorageError> {
        let table_name = scan.table_name.to_string();
        if !self.table.table_location.starts_with('/')
            && !self.table.table_location.starts_with("file:")
        {
            return Ok(TableScanSplitGroup::new(vec![TableScanSplit::new(
                table_name, 0,
            )]));
        }

        let table = self.build_table()?;
        let mut read_builder = table.new_read_builder();
        if let Some(limit) = scan.fetch {
            read_builder.with_limit(limit);
        }
        let planned_splits = block_on_storage_future(async move {
            read_builder
                .new_scan()
                .plan()
                .await
                .map(|plan| plan.splits().to_vec())
                .map_err(storage_scan_error)
        })?;
        if planned_splits.is_empty() {
            return Ok(TableScanSplitGroup::default());
        }

        let split_count = planned_splits.len();
        let plan_key = self.store_planned_splits(planned_splits);
        let splits = (0..split_count)
            .map(|ordinal| {
                TableScanSplit::new(table_name.clone(), ordinal as u32)
                    .with_property("paimon.plan_key", plan_key.clone())
            })
            .collect::<Vec<_>>();
        Ok(TableScanSplitGroup::new(splits))
    }
}

fn assigned_paimon_splits(splits: Vec<DataSplit>, ordinals: &[u32]) -> Vec<DataSplit> {
    if ordinals.is_empty() {
        return splits;
    }
    assigned_ordinal_indices(splits.len(), ordinals)
        .into_iter()
        .filter_map(|index| splits.get(index).cloned())
        .collect()
}

fn assigned_ordinal_indices(split_count: usize, ordinals: &[u32]) -> Vec<usize> {
    let mut ordinals = ordinals.to_vec();
    ordinals.sort_unstable();
    ordinals.dedup();
    ordinals
        .into_iter()
        .filter_map(|ordinal| {
            let index = ordinal as usize;
            (index < split_count).then_some(index)
        })
        .collect()
}

pub(crate) fn storage_scan_error(error: impl ToString) -> StorageError {
    StorageError::TableScanFailed {
        reason: error.to_string(),
    }
}

fn block_on_storage_future<T>(
    future: impl Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) | Err(_) => {
            let runtime = tokio::runtime::Runtime::new().map_err(storage_scan_error)?;
            runtime.block_on(future)
        }
    }
}

fn build_paimon_schema(table: &TableCatalogEntry) -> Result<PaimonTableSchema, StorageError> {
    let schema = PaimonSchemaAdapter::build_schema(
        &CreateTableRequest::new(
            table.path.database(),
            table.path.table(),
            table.table_schema.clone(),
        )
        .with_location(table.table_location.clone())
        .with_options(table.table_options.clone()),
    )
    .map_err(storage_scan_error)?;
    Ok(PaimonTableSchema::new(0, &schema))
}

#[cfg(test)]
mod tests {
    use super::assigned_ordinal_indices;

    #[test]
    fn assigned_ordinal_indices_keeps_only_requested_ordinals() {
        assert_eq!(assigned_ordinal_indices(3, &[2, 0, 2, 9]), vec![0, 2]);
    }
}
