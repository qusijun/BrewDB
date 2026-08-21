//! File-backed table engine for BrewDB file scans and COPY workflows.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::storage::{
    StorageError, TableEngine, TableEngineFactory, TableScanSplit, TableScanSplitGroup,
};
use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_expr::TableScan;

#[derive(Clone, Debug)]
pub struct FileTableEngine {
    location: String,
    is_directory: bool,
    file_format: Arc<dyn FileFormat>,
    listing_options: ListingOptions,
    schema: SchemaRef,
    listing_table: Arc<ListingTable>,
}

#[derive(Clone, Debug, Default)]
pub struct FileTableEngineFactory;

impl TableEngineFactory for FileTableEngineFactory {
    fn create_table_engine(
        &self,
        table: &TableCatalogEntry,
    ) -> Result<Arc<dyn TableEngine>, StorageError> {
        if table.storage_kind != StorageKind::File {
            return Err(StorageError::UnsupportedStorageKind {
                storage_kind: table.storage_kind.as_str().to_owned(),
            });
        }
        Ok(Arc::new(FileTableEngine::try_new(
            table.table_location.clone(),
            table.table_options.clone(),
        )?))
    }
}

fn open_file_table_engine_factory() -> Arc<dyn TableEngineFactory> {
    Arc::new(FileTableEngineFactory)
}

crate::register_table_engine_factory!(StorageKind::File, open_file_table_engine_factory);

impl FileTableEngine {
    pub fn try_new(
        location: impl Into<String>,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self, StorageError> {
        let location = location.into();
        let options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        let is_directory = fs::metadata(&location).map_err(storage_error)?.is_dir();
        let state = SessionContext::new().state();
        let table_path = ListingTableUrl::parse(location.clone()).map_err(storage_error)?;
        let config = build_listing_config(state, table_path, options)?;
        let listing_table = Arc::new(ListingTable::try_new(config.clone()).map_err(storage_error)?);
        let listing_options =
            config
                .options
                .clone()
                .ok_or_else(|| StorageError::TableScanFailed {
                    reason: "file listing options were not initialized".to_string(),
                })?;
        let schema = config
            .file_schema
            .clone()
            .ok_or_else(|| StorageError::TableScanFailed {
                reason: "file schema was not initialized".to_string(),
            })?;
        Ok(Self {
            location,
            is_directory,
            file_format: Arc::clone(&listing_options.format),
            listing_options,
            schema,
            listing_table,
        })
    }

    fn table_provider_for_paths(
        &self,
        paths: Vec<String>,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        if paths.is_empty() {
            return Ok(self.listing_table.clone());
        }
        Ok(Arc::new(self.listing_table_for_paths(paths)?))
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn file_format(&self) -> &Arc<dyn FileFormat> {
        &self.file_format
    }

    pub fn listing_options(&self) -> &ListingOptions {
        &self.listing_options
    }

    pub fn schema_ref(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn listing_table_for_paths(&self, paths: Vec<String>) -> Result<ListingTable, StorageError> {
        let paths = if paths.is_empty() {
            vec![self.location.clone()]
        } else {
            paths
        };
        let table_paths = paths
            .into_iter()
            .map(|path| ListingTableUrl::parse(path).map_err(storage_error))
            .collect::<Result<Vec<_>, StorageError>>()?;
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(self.listing_options.clone())
            .with_schema(Arc::clone(&self.schema));
        ListingTable::try_new(config).map_err(storage_error)
    }
}

impl TableEngine for FileTableEngine {
    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider_for_paths(Vec::new())
    }

    fn schema_ref(&self) -> Result<SchemaRef, StorageError> {
        Ok(Arc::clone(&self.schema))
    }

    fn get_table_provider(
        &self,
        splits: &TableScanSplitGroup,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider_for_paths(
            splits
                .splits
                .iter()
                .flat_map(|split| split.locations.iter().cloned())
                .collect(),
        )
    }

    fn plan_scan(&self, scan: &TableScan) -> Result<TableScanSplitGroup, StorageError> {
        let table_name = scan.table_name.to_string();
        let format = self.file_format.get_ext();
        let listing_table = Arc::clone(&self.listing_table);
        let location = self.location.clone();
        let is_directory = self.is_directory;
        block_on_storage_future(async move {
            let state = SessionContext::new().state();
            let files = listing_table
                .list_files_for_scan(&state, &[], None)
                .await
                .map_err(storage_error)?
                .file_groups
                .into_iter()
                .flat_map(|group| group.into_inner())
                .map(|file| {
                    normalize_split_location(
                        &location,
                        is_directory,
                        file.object_meta.location.to_string(),
                    )
                })
                .enumerate()
                .map(|(ordinal, path)| {
                    TableScanSplit::new(table_name.clone(), ordinal as u32)
                        .with_locations([path])
                        .with_property("format", format.clone())
                })
                .collect();
            Ok(TableScanSplitGroup::new(files))
        })
    }
}

fn build_listing_config(
    state: SessionState,
    table_path: ListingTableUrl,
    options: BTreeMap<String, String>,
) -> Result<ListingTableConfig, StorageError> {
    let config = ListingTableConfig::new_with_multi_paths(vec![table_path]);
    block_on_storage_future(async move {
        let config = config.infer_options(&state).await.map_err(storage_error)?;
        let config = apply_format_options(config, &state, &options)?;
        config.infer_schema(&state).await.map_err(storage_error)
    })
}

fn apply_format_options(
    config: ListingTableConfig,
    state: &SessionState,
    options: &BTreeMap<String, String>,
) -> Result<ListingTableConfig, StorageError> {
    let format_options = options
        .iter()
        .filter_map(|(key, value)| match key.as_str() {
            "format" | "file_type" => None,
            "has_header" => Some(("format.has_header".to_owned(), value.clone())),
            key if key.starts_with("format.") => Some((key.to_owned(), value.clone())),
            key => Some((format!("format.{key}"), value.clone())),
        })
        .collect::<std::collections::HashMap<_, _>>();
    if format_options.is_empty() {
        return Ok(config);
    }
    let Some(listing_options) = config.options.clone() else {
        return Err(StorageError::TableScanFailed {
            reason: "file listing options were not initialized".to_string(),
        });
    };
    let inferred_file_type = listing_options.format.get_ext();
    let factory = state
        .get_file_format_factory(&inferred_file_type)
        .ok_or_else(|| StorageError::TableScanFailed {
            reason: format!("unsupported file format: {inferred_file_type}"),
        })?;
    let file_format = factory
        .create(state, &format_options)
        .map_err(storage_error)?;
    let listing_options = ListingOptions::new(file_format)
        .with_file_extension(listing_options.file_extension)
        .with_target_partitions(listing_options.target_partitions)
        .with_collect_stat(listing_options.collect_stat)
        .with_table_partition_cols(listing_options.table_partition_cols)
        .with_file_sort_order(listing_options.file_sort_order);
    Ok(config.with_listing_options(listing_options))
}

fn block_on_storage_future<T>(
    future: impl Future<Output = Result<T, StorageError>> + Send + 'static,
) -> Result<T, StorageError>
where
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().map_err(storage_error)?;
            runtime.block_on(future)
        })
        .join()
        .map_err(|_| StorageError::TableScanFailed {
            reason: "file schema inference thread panicked".to_string(),
        })?,
        Err(_) => {
            let runtime = tokio::runtime::Runtime::new().map_err(storage_error)?;
            runtime.block_on(future)
        }
    }
}

fn storage_error(error: impl ToString) -> StorageError {
    StorageError::TableScanFailed {
        reason: error.to_string(),
    }
}

fn normalize_split_location(root_location: &str, is_directory: bool, path: String) -> String {
    if Path::new(&path).is_absolute() || path.contains("://") {
        return path;
    }
    let absolute_candidate = format!("/{path}");
    if Path::new(&absolute_candidate).exists() {
        return absolute_candidate;
    }
    if !is_directory {
        if let Some(parent) = Path::new(root_location).parent() {
            return parent.join(path).to_string_lossy().into_owned();
        }
    }
    Path::new(root_location)
        .join(path)
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::{TableProvider, provider_as_source};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use std::fs;

    use crate::catalog::TableCatalogEntry;
    use crate::storage::{TableEngine, TableEngineFactory, TableScanSplit, TableScanSplitGroup};

    use super::{FileTableEngine, FileTableEngineFactory};

    #[test]
    fn file_table_engine_initializes_listing_components_from_path_and_options() {
        let path =
            std::env::temp_dir().join(format!("brewdb-file-engine-{}.csv", uuid::Uuid::new_v4()));
        fs::write(&path, "id,name\n1,apple\n").unwrap();

        let engine =
            FileTableEngine::try_new(path.to_string_lossy().to_string(), [("has_header", "true")])
                .unwrap();

        assert_eq!(engine.location(), path.to_string_lossy());
        assert_eq!(engine.file_format.get_ext(), "csv");
        assert_eq!(engine.listing_options.file_extension, "csv");
        assert_eq!(engine.schema_ref().field(0).name(), "id");
        assert_eq!(engine.listing_table.schema().field(0).name(), "id");
    }

    #[test]
    fn file_table_engine_listing_table_reads_csv_data() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path =
                std::env::temp_dir().join(format!("brewdb-file-{}.csv", uuid::Uuid::new_v4()));
            fs::write(&path, "id,name\n1,apple\n2,banana\n").unwrap();

            let engine = FileTableEngine::try_new(
                path.to_string_lossy().to_string(),
                [("has_header", "true")],
            )
            .unwrap();
            let provider = engine.table_provider().unwrap();
            let projection = vec![0];
            let exec = provider
                .scan(&SessionContext::new().state(), Some(&projection), &[], None)
                .await
                .unwrap();

            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].schema().field(0).name(), "id");
        });
    }

    #[test]
    fn file_table_engine_factory_infers_csv_format_from_file_location() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = std::env::temp_dir()
                .join(format!("brewdb-file-infer-{}.csv", uuid::Uuid::new_v4()));
            fs::write(&path, "id\n9\n").unwrap();
            let table = TableCatalogEntry::temporary_file(
                "copy_source",
                path.to_string_lossy().to_string(),
                [("has_header", "true")],
            )
            .unwrap();

            let provider = FileTableEngineFactory
                .create_table_engine(&table)
                .unwrap()
                .table_provider()
                .unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(table.table_options.get("file_type"), None);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].schema().field(0).name(), "id");
        });
    }

    #[test]
    fn file_table_engine_factory_opens_directory_table_sources() {
        let dir =
            std::env::temp_dir().join(format!("brewdb-file-dir-source-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("part-1.csv"), "id\n11\n").unwrap();
        let table = TableCatalogEntry::temporary_file(
            "directory_source",
            dir.to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        let engine = FileTableEngineFactory.create_table_engine(&table).unwrap();
        assert_eq!(engine.schema_ref().unwrap().field(0).name(), "id");
    }

    #[test]
    fn file_table_engine_plans_directory_files_as_splits() {
        let dir = std::env::temp_dir().join(format!("brewdb-file-dir-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("part-1.csv"), "id\n1\n").unwrap();
        fs::write(dir.join("part-2.csv"), "id\n2\n").unwrap();

        let engine =
            FileTableEngine::try_new(dir.to_string_lossy().to_string(), [("has_header", "true")])
                .unwrap();
        let scan = datafusion_expr::TableScan::try_new(
            datafusion_common::TableReference::bare("source"),
            provider_as_source(engine.table_provider().unwrap()),
            None,
            vec![],
            None,
        )
        .unwrap();
        let splits = engine.plan_scan(&scan).unwrap();

        assert_eq!(splits.splits.len(), 2);
        assert!(splits.splits[0].locations[0].ends_with("part-1.csv"));
        assert!(splits.splits[1].locations[0].ends_with("part-2.csv"));
    }

    #[test]
    fn file_table_engine_get_table_provider_reads_assigned_splits_only() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let dir =
                std::env::temp_dir().join(format!("brewdb-file-assigned-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("part-1.csv"), "id\n1\n").unwrap();
            fs::write(dir.join("part-2.csv"), "id\n2\n").unwrap();

            let engine = FileTableEngine::try_new(
                dir.to_string_lossy().to_string(),
                [("has_header", "true")],
            )
            .unwrap();
            let provider = engine
                .get_table_provider(&TableScanSplitGroup::new(vec![
                    TableScanSplit::new("source", 0)
                        .with_locations([dir.join("part-2.csv").display().to_string()]),
                ]))
                .unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(batches[0].num_rows(), 1);
        });
    }

    #[test]
    fn file_table_engine_planned_single_file_split_reads_back() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = std::env::temp_dir().join(format!(
                "brewdb-file-single-split-{}.csv",
                uuid::Uuid::new_v4()
            ));
            fs::write(&path, "id\n1\n2\n3\n").unwrap();
            let engine = FileTableEngine::try_new(
                path.to_string_lossy().to_string(),
                [("has_header", "true")],
            )
            .unwrap();
            let scan = datafusion_expr::TableScan::try_new(
                datafusion_common::TableReference::bare("source"),
                provider_as_source(engine.table_provider().unwrap()),
                None,
                vec![],
                None,
            )
            .unwrap();
            let splits = engine.plan_scan(&scan).unwrap();
            assert_eq!(splits.splits.len(), 1);
            let provider = engine.get_table_provider(&splits).unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3,
                "split locations: {:?}",
                splits
                    .splits
                    .iter()
                    .flat_map(|split| split.locations.iter())
                    .collect::<Vec<_>>()
            );
        });
    }
}
