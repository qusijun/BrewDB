//! File-backed table engine for BrewDB file scans and COPY workflows.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::sync::Arc;
use std::thread;

use crate::catalog::{StorageKind, TableCatalogEntry};
use crate::file::util::{file_extension, normalize_split_location, table_name};
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
    table_name: String,
    location: String,
    is_directory: bool,
    file_format: Arc<dyn FileFormat>,
    listing_options: ListingOptions,
    schema: SchemaRef,
    listing_table: Arc<ListingTable>,
}

#[derive(Clone, Debug, Default)]
pub struct FileTableEngineFactory;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileTableLocationKind {
    Directory,
    File,
}

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
        let expected_table_schema = if table.table_schema.fields.is_empty() {
            None
        } else {
            Some(
                table
                    .table_schema
                    .to_arrow_schema_ref()
                    .map_err(storage_error)?,
            )
        };
        Ok(Arc::new(
            FileTableEngine::try_new_with_expected_table_schema(
                table.table_location.clone(),
                table.table_options.clone(),
                expected_table_schema,
            )?,
        ))
    }
}

fn open_file_table_engine_factory() -> Arc<dyn TableEngineFactory> {
    Arc::new(FileTableEngineFactory)
}

crate::register_table_engine_factory!(StorageKind::File, open_file_table_engine_factory);

impl FileTableEngine {
    pub fn classify_location(location: &str) -> Result<FileTableLocationKind, StorageError> {
        fs::metadata(location)
            .map(|metadata| {
                if metadata.is_dir() {
                    FileTableLocationKind::Directory
                } else {
                    FileTableLocationKind::File
                }
            })
            .map_err(storage_error)
    }

    pub fn try_new(
        location: impl Into<String>,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Result<Self, StorageError> {
        Self::try_new_with_expected_table_schema(location, options, None)
    }

    /// Creates a file engine whose scan output follows the expected table
    /// schema when provided.
    ///
    /// COPY FROM uses this path to let the target table define the reader's
    /// output names and types, matching DuckDB's expected schema driven bind
    /// flow. Ordinary file scans pass `None` and infer the schema from files.
    pub fn try_new_with_expected_table_schema(
        location: impl Into<String>,
        options: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
        expected_table_schema: Option<SchemaRef>,
    ) -> Result<Self, StorageError> {
        let location = location.into();
        let table_name = table_name(&location);
        let options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        let is_directory = fs::metadata(&location).map_err(storage_error)?.is_dir();
        let state = SessionContext::new().state();
        let table_path = ListingTableUrl::parse(location.clone()).map_err(storage_error)?;
        let config = build_listing_config(state, table_path, options, expected_table_schema)?;
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
            table_name,
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

    pub fn location_kind(&self) -> FileTableLocationKind {
        if self.is_directory {
            FileTableLocationKind::Directory
        } else {
            FileTableLocationKind::File
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
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
    fn storage_kind(&self) -> StorageKind {
        StorageKind::File
    }

    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider_for_paths(Vec::new())
    }

    fn schema_ref(&self) -> Result<SchemaRef, StorageError> {
        Ok(Arc::clone(&self.schema))
    }

    fn get_table_provider(
        &self,
        split: Option<&TableScanSplit>,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        self.table_provider_for_paths(
            split
                .into_iter()
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
    expected_table_schema: Option<SchemaRef>,
) -> Result<ListingTableConfig, StorageError> {
    let config = ListingTableConfig::new_with_multi_paths(vec![table_path]);
    block_on_storage_future(async move {
        let config = match expected_file_type(&options) {
            Some(file_type) => apply_format_options(config, &state, &options, file_type)?,
            None => {
                let config = config.infer_options(&state).await.map_err(storage_error)?;
                let inferred_file_type = config
                    .options
                    .as_ref()
                    .map(|options| options.format.get_ext())
                    .ok_or_else(|| StorageError::TableScanFailed {
                        reason: "file listing options were not initialized".to_string(),
                    })?;
                apply_format_options(config, &state, &options, &inferred_file_type)?
            }
        };
        match expected_table_schema {
            Some(expected_table_schema) => Ok(config.with_schema(expected_table_schema)),
            None => config.infer_schema(&state).await.map_err(storage_error),
        }
    })
}

fn expected_file_type(options: &BTreeMap<String, String>) -> Option<&str> {
    options
        .get("format")
        .or_else(|| options.get("file_type"))
        .map(String::as_str)
}

fn apply_format_options(
    config: ListingTableConfig,
    state: &SessionState,
    options: &BTreeMap<String, String>,
    file_type: &str,
) -> Result<ListingTableConfig, StorageError> {
    let format_options = options
        .iter()
        .filter_map(|(key, value)| match key.as_str() {
            "format" | "file_type" => None,
            "has_header" if file_type == "csv" => {
                Some(("format.has_header".to_owned(), value.clone()))
            }
            "has_header" => None,
            key if key.starts_with("format.") => Some((key.to_owned(), value.clone())),
            key => Some((format!("format.{key}"), value.clone())),
        })
        .collect::<std::collections::HashMap<_, _>>();
    if format_options.is_empty() && config.options.is_some() {
        return Ok(config);
    }
    let factory =
        state
            .get_file_format_factory(file_type)
            .ok_or_else(|| StorageError::TableScanFailed {
                reason: format!("unsupported file format: {file_type}"),
            })?;
    let file_format = factory
        .create(state, &format_options)
        .map_err(storage_error)?;
    let listing_options = match config.options.clone() {
        Some(listing_options) => ListingOptions::new(file_format)
            .with_file_extension(listing_options.file_extension)
            .with_target_partitions(listing_options.target_partitions)
            .with_collect_stat(listing_options.collect_stat)
            .with_table_partition_cols(listing_options.table_partition_cols)
            .with_file_sort_order(listing_options.file_sort_order),
        None => ListingOptions::new(file_format)
            .with_file_extension(file_extension(
                config
                    .table_paths
                    .first()
                    .map(|table_path| table_path.as_str())
                    .unwrap_or_default(),
                file_type,
            ))
            .with_target_partitions(state.config().target_partitions())
            .with_collect_stat(state.config().collect_statistics()),
    };
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

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_common::test_util::{TestDir, TestFile, write_parquet_file};
    use brewdb_common::{column::ColumnField, datatype::DataType, table::TableSchema};
    use datafusion::datasource::{TableProvider, provider_as_source};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use std::fs;
    use std::sync::Arc;

    use crate::catalog::TableCatalogEntry;
    use crate::storage::{TableEngine, TableEngineFactory, TableScanSplit};

    use super::{FileTableEngine, FileTableEngineFactory, FileTableLocationKind};

    fn parquet_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["apple", "banana"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn file_table_engine_initializes_listing_components_from_path_and_options() {
        let path = TestFile::new("brewdb-file-engine", "csv");
        fs::write(path.path(), "id,name\n1,apple\n").unwrap();

        let engine = FileTableEngine::try_new(
            path.path().to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        assert_eq!(engine.location(), path.path().to_string_lossy());
        assert_eq!(engine.file_format.get_ext(), "csv");
        assert_eq!(engine.listing_options.file_extension, "csv");
        assert_eq!(engine.schema_ref().field(0).name(), "id");
        assert_eq!(engine.listing_table.schema().field(0).name(), "id");
    }

    #[test]
    fn file_table_engine_listing_table_reads_csv_data() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = TestFile::new("brewdb-file", "csv");
            fs::write(path.path(), "id,name\n1,apple\n2,banana\n").unwrap();

            let engine = FileTableEngine::try_new(
                path.path().to_string_lossy().to_string(),
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
            let path = TestFile::new("brewdb-file-infer", "csv");
            fs::write(path.path(), "id\n9\n").unwrap();
            let table = TableCatalogEntry::temporary_file(
                "copy_source",
                path.path().to_string_lossy().to_string(),
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
    fn file_table_engine_reports_single_file_locations() {
        let path = TestFile::new("brewdb-file-single-location", "csv");
        fs::write(path.path(), "id\n1\n").unwrap();

        assert_eq!(
            FileTableEngine::classify_location(path.path().to_string_lossy().as_ref()).unwrap(),
            FileTableLocationKind::File
        );
    }

    #[test]
    fn file_table_engine_reports_directory_locations() {
        let dir = TestDir::new("brewdb-file-directory-location");

        assert_eq!(
            FileTableEngine::classify_location(dir.path().to_string_lossy().as_ref()).unwrap(),
            FileTableLocationKind::Directory
        );
    }

    #[test]
    fn file_table_engine_derives_table_name_from_file_location() {
        let path = TestFile::new("brewdb-file-table-name", "csv");
        fs::write(path.path(), "id\n1\n").unwrap();

        let engine = FileTableEngine::try_new(
            path.path().to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        assert!(engine.table_name().starts_with("brewdb_file_table_name_"));
        assert!(engine.table_name().ends_with("_csv"));
    }

    #[test]
    fn file_table_engine_infers_parquet_format_from_file_location() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = TestFile::new("brewdb-file-infer-parquet", "parquet");
            write_parquet_file(path.path(), parquet_test_batch());

            let engine = FileTableEngine::try_new(
                path.path().to_string_lossy().to_string(),
                [] as [(&str, &str); 0],
            )
            .unwrap();
            let provider = engine.table_provider().unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(engine.file_format.get_ext(), "parquet");
            assert_eq!(engine.listing_options.file_extension, "parquet");
            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].schema().field(0).name(), "id");
        });
    }

    #[test]
    fn file_table_engine_explicit_parquet_format_overrides_file_extension() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let path = TestFile::new("brewdb-file-explicit-parquet", "data");
            write_parquet_file(path.path(), parquet_test_batch());

            let engine = FileTableEngine::try_new(
                path.path().to_string_lossy().to_string(),
                [("format", "parquet")],
            )
            .unwrap();
            let provider = engine.table_provider().unwrap();
            let exec = provider
                .scan(&SessionContext::new().state(), None, &[], None)
                .await
                .unwrap();
            let ctx = SessionContext::new();
            let batches = collect(exec, ctx.task_ctx()).await.unwrap();

            assert_eq!(engine.file_format.get_ext(), "parquet");
            assert_eq!(engine.listing_options.file_extension, "data");
            assert_eq!(batches[0].num_rows(), 2);
        });
    }

    #[test]
    fn file_table_engine_uses_expected_table_schema_when_provided() {
        let path = TestFile::new("brewdb-file-catalog-schema", "csv");
        fs::write(path.path(), "\n").unwrap();
        let expected_table_schema = TableSchema::new(vec![ColumnField::new("id", DataType::Int32)])
            .to_arrow_schema_ref()
            .unwrap();

        let engine = FileTableEngine::try_new_with_expected_table_schema(
            path.path().to_string_lossy().to_string(),
            [("has_header", "false")],
            Some(expected_table_schema),
        )
        .unwrap();

        assert_eq!(engine.schema_ref().field(0).name(), "id");
        assert_eq!(
            engine.schema_ref().field(0).data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }

    #[test]
    fn file_table_engine_factory_uses_catalog_schema_when_present() {
        let path = TestFile::new("brewdb-file-factory-expected-schema", "csv");
        fs::write(path.path(), "20\n").unwrap();
        let table = TableCatalogEntry::temporary_file_with_schema(
            "copy_source",
            path.path().to_string_lossy().to_string(),
            TableSchema::new(vec![ColumnField::new("id", DataType::Int32)]),
            [("has_header", "false")],
        )
        .unwrap();

        let engine = FileTableEngineFactory.create_table_engine(&table).unwrap();

        assert_eq!(engine.schema_ref().unwrap().field(0).name(), "id");
        assert_eq!(
            engine.schema_ref().unwrap().field(0).data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }

    #[test]
    fn file_table_engine_factory_opens_directory_table_sources() {
        let dir = TestDir::new("brewdb-file-dir-source");
        fs::write(dir.path().join("part-1.csv"), "id\n11\n").unwrap();
        let table = TableCatalogEntry::temporary_file(
            "directory_source",
            dir.path().to_string_lossy().to_string(),
            [("has_header", "true")],
        )
        .unwrap();

        let engine = FileTableEngineFactory.create_table_engine(&table).unwrap();
        assert_eq!(engine.schema_ref().unwrap().field(0).name(), "id");
    }

    #[test]
    fn file_table_engine_plans_directory_files_as_splits() {
        let dir = TestDir::new("brewdb-file-dir");
        fs::write(dir.path().join("part-1.csv"), "id\n1\n").unwrap();
        fs::write(dir.path().join("part-2.csv"), "id\n2\n").unwrap();

        let engine = FileTableEngine::try_new(
            dir.path().to_string_lossy().to_string(),
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
        let splits = splits.only_table_source_splits().unwrap();

        assert_eq!(splits.len(), 2);
        assert!(splits[0].locations[0].ends_with("part-1.csv"));
        assert!(splits[1].locations[0].ends_with("part-2.csv"));
    }

    #[test]
    fn file_table_engine_get_table_provider_reads_assigned_splits_only() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let dir = TestDir::new("brewdb-file-assigned");
            fs::write(dir.path().join("part-1.csv"), "id\n1\n").unwrap();
            fs::write(dir.path().join("part-2.csv"), "id\n2\n").unwrap();

            let engine = FileTableEngine::try_new(
                dir.path().to_string_lossy().to_string(),
                [("has_header", "true")],
            )
            .unwrap();
            let provider = engine
                .get_table_provider(Some(
                    &TableScanSplit::new("source", 0).with_locations([dir
                        .path()
                        .join("part-2.csv")
                        .display()
                        .to_string()]),
                ))
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
            let path = TestFile::new("brewdb-file-single-split", "csv");
            fs::write(path.path(), "id\n1\n2\n3\n").unwrap();
            let engine = FileTableEngine::try_new(
                path.path().to_string_lossy().to_string(),
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
            let only_splits = splits.only_table_source_splits().unwrap();
            assert_eq!(only_splits.len(), 1);
            let provider = engine.get_table_provider(only_splits.first()).unwrap();
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
                only_splits
                    .iter()
                    .flat_map(|split| split.locations.iter())
                    .collect::<Vec<_>>()
            );
        });
    }
}
