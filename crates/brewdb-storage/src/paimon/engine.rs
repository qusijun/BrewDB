use std::future::Future;
use std::sync::Arc;

use crate::catalog::{
    CreateTableRequest, PaimonSchemaAdapter, StorageFormatSchemaAdapter, StorageKind,
    TableCatalogEntry,
};
use crate::storage::{
    BucketDescriptor, DataFileDescriptor, DataFileFormat, DeletionFileDescriptor,
    PartitionDescriptor, RowRangeDescriptor, StorageError, TableEngine, TableEngineFactory,
    TableScanSplit, TableScanSplitGroup,
};
use datafusion::datasource::TableProvider;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableScan};
use paimon::catalog::Identifier as PaimonIdentifier;
use paimon::io::FileIO;
use paimon::spec::Predicate;
use paimon::spec::{BinaryRow, DataFileMeta, TableSchema as PaimonTableSchema};
use paimon::table::Table as PaimonTable;
use paimon::{DataSplit, DataSplitBuilder, DeletionFile, RowRange};

use super::predicate::{filter_predicates, filter_pushdown_status};
use super::table_provider::PaimonTableProvider;

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

    /// Normalizes a DataFusion table scan into storage-side scan constraints.
    ///
    /// This is intentionally private to the Paimon engine: the shape of a
    /// pruning result is storage-specific, while BrewDB's public boundary is
    /// still `plan_scan`. The projection remains in BrewDB/DataFusion index
    /// form here and is translated to Paimon column names only at the Paimon
    /// `ReadBuilder` boundary.
    fn pruning(
        &self,
        table: &PaimonTable,
        scan: &TableScan,
    ) -> Result<PaimonPruning, StorageError> {
        validate_projection(table.schema(), scan.projection.as_deref())?;
        let filters = filter_predicates(table.schema().fields(), &scan.filters);
        let filter = (!filters.is_empty()).then(|| Predicate::and(filters));
        Ok(PaimonPruning {
            projection: scan.projection.clone(),
            filter,
            limit: scan.fetch,
        })
    }
}

struct PaimonPruning {
    projection: Option<Vec<usize>>,
    filter: Option<Predicate>,
    limit: Option<usize>,
}

impl TableEngine for PaimonTableEngine {
    fn storage_kind(&self) -> StorageKind {
        StorageKind::Paimon
    }

    fn table_provider(&self) -> Result<Arc<dyn TableProvider>, StorageError> {
        Ok(Arc::new(PaimonTableProvider::try_new(
            self.build_table()?,
            None,
        )?))
    }

    fn get_table_provider(
        &self,
        split: Option<&TableScanSplit>,
    ) -> Result<Arc<dyn TableProvider>, StorageError> {
        let planned_splits = split
            .map(paimon_split_from_table_scan_split)
            .transpose()?
            .map(|split| vec![split]);
        Ok(Arc::new(PaimonTableProvider::try_new(
            self.build_table()?,
            planned_splits,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let schema = build_paimon_schema(&self.table)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(filters
            .iter()
            .map(|filter| {
                match filter_pushdown_status(schema.fields(), schema.partition_keys(), filter) {
                    Some((_predicate, exact)) => {
                        if exact {
                            TableProviderFilterPushDown::Exact
                        } else {
                            TableProviderFilterPushDown::Inexact
                        }
                    }
                    None => TableProviderFilterPushDown::Unsupported,
                }
            })
            .collect())
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
        let pruning = self.pruning(&table, scan)?;
        let mut read_builder = table.new_read_builder();
        if let Some(projection) = &pruning.projection {
            let projection_names = projection_names(table.schema(), projection)?;
            let projection = projection_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            read_builder.with_projection(&projection);
        }
        if let Some(filter) = pruning.filter {
            read_builder.with_filter(filter);
        }
        if let Some(limit) = pruning.limit {
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

        let splits = planned_splits
            .iter()
            .enumerate()
            .map(|(ordinal, split)| {
                table_scan_split_from_paimon_split(table_name.clone(), ordinal as u32, split)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TableScanSplitGroup::new(splits))
    }
}

fn validate_projection(
    schema: &PaimonTableSchema,
    projection: Option<&[usize]>,
) -> Result<(), StorageError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    for index in projection {
        if schema.fields().get(*index).is_none() {
            return Err(StorageError::TableScanFailed {
                reason: format!("invalid Paimon scan projection index {index}"),
            });
        }
    }
    Ok(())
}

fn projection_names(
    schema: &PaimonTableSchema,
    projection: &[usize],
) -> Result<Vec<String>, StorageError> {
    projection
        .iter()
        .map(|index| {
            schema
                .fields()
                .get(*index)
                .map(|field| field.name().to_owned())
                .ok_or_else(|| StorageError::TableScanFailed {
                    reason: format!("invalid Paimon scan projection index {index}"),
                })
        })
        .collect()
}

fn table_scan_split_from_paimon_split(
    table_name: String,
    ordinal: u32,
    split: &DataSplit,
) -> Result<TableScanSplit, StorageError> {
    let data_files = split
        .data_files()
        .iter()
        .map(|file| {
            let path = data_file_path(split.bucket_path(), &file.file_name);
            Ok(DataFileDescriptor {
                file_format: DataFileFormat::from_path(&path),
                file_name: file.file_name.clone(),
                path,
                file_size: u64::try_from(file.file_size).ok(),
                row_count: u64::try_from(file.row_count).ok(),
                schema_id: Some(file.schema_id),
                serialized_metadata: Some(serde_json::to_vec(file).map_err(storage_scan_error)?),
                min_sequence_number: Some(file.min_sequence_number),
                max_sequence_number: Some(file.max_sequence_number),
                delete_row_count: file.delete_row_count,
                first_row_id: file.first_row_id,
                external_path: file.external_path.clone(),
                write_columns: file.write_cols.clone(),
                extra_files: file.extra_files.clone(),
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;

    Ok(TableScanSplit::new(table_name, ordinal)
        .with_snapshot_id(split.snapshot_id())
        .with_partition(PartitionDescriptor {
            serialized_binary_row: split.partition().to_serialized_bytes(),
        })
        .with_bucket(BucketDescriptor {
            bucket: split.bucket(),
            total_buckets: Some(split.total_buckets()),
            path: Some(split.bucket_path().to_owned()),
        })
        .with_data_files(data_files)
        .with_deletion_files(
            split
                .data_deletion_files()
                .into_iter()
                .flatten()
                .enumerate()
                .filter_map(|(index, file)| file.as_ref().map(|file| (index, file)))
                .map(|(index, file)| DeletionFileDescriptor {
                    data_file_ordinal: index as u32,
                    path: file.path().to_owned(),
                    offset: file.offset(),
                    length: file.length(),
                    row_count: file
                        .cardinality()
                        .and_then(|value| u64::try_from(value).ok()),
                })
                .collect(),
        )
        .with_row_ranges(
            split
                .row_ranges()
                .into_iter()
                .flatten()
                .map(|range| RowRangeDescriptor {
                    from: range.from(),
                    to: range.to(),
                })
                .collect(),
        ))
}

fn paimon_split_from_table_scan_split(split: &TableScanSplit) -> Result<DataSplit, StorageError> {
    let snapshot_id = split
        .snapshot_id
        .ok_or_else(|| StorageError::TableScanFailed {
            reason: format!("Paimon scan split {} misses snapshot id", split.split_id),
        })?;
    let partition = split
        .partition
        .as_ref()
        .ok_or_else(|| StorageError::TableScanFailed {
            reason: format!("Paimon scan split {} misses partition", split.split_id),
        })
        .and_then(|partition| {
            BinaryRow::from_serialized_bytes(&partition.serialized_binary_row)
                .map_err(storage_scan_error)
        })?;
    let bucket = split
        .bucket
        .as_ref()
        .ok_or_else(|| StorageError::TableScanFailed {
            reason: format!("Paimon scan split {} misses bucket", split.split_id),
        })?;
    let bucket_path = bucket
        .path
        .clone()
        .ok_or_else(|| StorageError::TableScanFailed {
            reason: format!("Paimon scan split {} misses bucket path", split.split_id),
        })?;
    let data_files = split
        .data_files
        .iter()
        .map(data_file_meta_from_descriptor)
        .collect::<Result<Vec<_>, StorageError>>()?;

    let mut builder = DataSplitBuilder::new()
        .with_snapshot(snapshot_id)
        .with_partition(partition)
        .with_bucket(bucket.bucket)
        .with_bucket_path(bucket_path)
        .with_total_buckets(bucket.total_buckets.unwrap_or(1))
        .with_data_files(data_files);

    if !split.deletion_files.is_empty() {
        let mut deletion_files = vec![None; split.data_files.len()];
        for deletion_file in &split.deletion_files {
            let index = deletion_file.data_file_ordinal as usize;
            if index < deletion_files.len() {
                deletion_files[index] = Some(DeletionFile::new(
                    deletion_file.path.clone(),
                    deletion_file.offset,
                    deletion_file.length,
                    deletion_file
                        .row_count
                        .and_then(|value| i64::try_from(value).ok()),
                ));
            }
        }
        builder = builder.with_data_deletion_files(deletion_files);
    }

    if !split.row_ranges.is_empty() {
        builder = builder.with_row_ranges(
            split
                .row_ranges
                .iter()
                .map(|range| RowRange::new(range.from, range.to))
                .collect(),
        );
    }

    builder.build().map_err(storage_scan_error)
}

fn data_file_meta_from_descriptor(file: &DataFileDescriptor) -> Result<DataFileMeta, StorageError> {
    let metadata =
        file.serialized_metadata
            .as_deref()
            .ok_or_else(|| StorageError::TableScanFailed {
                reason: format!("Paimon data file {} misses serialized metadata", file.path),
            })?;
    serde_json::from_slice(metadata).map_err(storage_scan_error)
}

fn data_file_path(bucket_path: &str, file_name: &str) -> String {
    if file_name.starts_with("file:") || file_name.starts_with('/') {
        return file_name.to_owned();
    }
    format!("{}/{}", bucket_path.trim_end_matches('/'), file_name)
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
