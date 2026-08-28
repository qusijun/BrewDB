use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, stream};
use paimon::spec::Predicate;
use paimon::table::Table as PaimonTable;
use paimon::{DataSplit, DataSplitBuilder};

use super::table_provider::datafusion_scan_error;
use brewdb_common::profile::MetricValue;

#[derive(Debug)]
pub(crate) struct PaimonScanExec {
    table: PaimonTable,
    projected_schema: SchemaRef,
    partitions: Vec<Arc<[DataSplit]>>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
    limit: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    storage_rows: Count,
    storage_bytes: Count,
    properties: Arc<PlanProperties>,
}

impl PaimonScanExec {
    pub(crate) fn new(
        table: PaimonTable,
        projected_schema: SchemaRef,
        assigned_splits: Vec<DataSplit>,
        projection: Option<Vec<String>>,
        filter: Option<Predicate>,
        limit: Option<usize>,
        target_partitions: usize,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let partitions = scan_partitions(assigned_splits, target_partitions);
        let storage_files = partitions
            .iter()
            .map(|partition| storage_files_count(partition))
            .sum();
        MetricBuilder::new(&metrics)
            .global_counter(MetricValue::STORAGE_FILES_READ)
            .add(storage_files);
        let storage_rows =
            MetricBuilder::new(&metrics).global_counter(MetricValue::STORAGE_ROWS_READ);
        let storage_bytes =
            MetricBuilder::new(&metrics).global_counter(MetricValue::STORAGE_BYTES_READ);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            projected_schema,
            partitions,
            projection,
            filter,
            limit,
            metrics,
            storage_rows,
            storage_bytes,
            properties,
        }
    }
}

impl ExecutionPlan for PaimonScanExec {
    fn name(&self) -> &str {
        "PaimonScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let Some(scan_partition) = self.partitions.get(partition) else {
            return Err(DataFusionError::Internal(format!(
                "PaimonScanExec invalid partition {partition}, expected less than {}",
                self.partitions.len()
            )));
        };

        let mut read_builder = self.table.new_read_builder();
        if let Some(projection) = &self.projection {
            let projection: Vec<&str> = projection.iter().map(String::as_str).collect();
            read_builder.with_projection(&projection);
        }
        if let Some(filter) = &self.filter {
            read_builder.with_filter(filter.clone());
        }
        if let Some(limit) = self.limit {
            read_builder.with_limit(limit);
        }
        let result = read_builder
            .new_read()
            .map_err(datafusion_scan_error)
            .and_then(|read| read.to_arrow(scan_partition).map_err(datafusion_scan_error));

        let schema = Arc::clone(&self.projected_schema);
        let mut remaining = self.limit;
        let storage_rows = self.storage_rows.clone();
        let storage_bytes = self.storage_bytes.clone();
        match result {
            Ok(batch_stream) => Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                batch_stream.map(move |batch| {
                    let mut batch = batch.map_err(datafusion_scan_error)?;
                    if let Some(remaining_rows) = remaining.as_mut() {
                        let rows_to_take = (*remaining_rows).min(batch.num_rows());
                        *remaining_rows -= rows_to_take;
                        batch = batch.slice(0, rows_to_take);
                    }
                    storage_rows.add(batch.num_rows());
                    storage_bytes.add(batch.get_array_memory_size());
                    Ok(batch)
                }),
            ))),
            Err(error) => Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(async move { Err(error) }),
            ))),
        }
    }
}

fn storage_files_count(scan_splits: &[DataSplit]) -> usize {
    scan_splits
        .iter()
        .map(|split| split.data_files().len())
        .sum()
}

fn scan_partitions(splits: Vec<DataSplit>, target_partitions: usize) -> Vec<Arc<[DataSplit]>> {
    let splits = scan_work_units(splits);
    if splits.is_empty() {
        return Vec::new();
    }

    let partition_count = splits.len().min(target_partitions.max(1));
    let mut partitions = (0..partition_count).map(|_| Vec::new()).collect::<Vec<_>>();
    for (index, split) in splits.into_iter().enumerate() {
        partitions[index % partition_count].push(split);
    }
    partitions.into_iter().map(Arc::from).collect()
}

fn scan_work_units(splits: Vec<DataSplit>) -> Vec<DataSplit> {
    splits.into_iter().flat_map(split_scan_work_units).collect()
}

fn split_scan_work_units(split: DataSplit) -> Vec<DataSplit> {
    if split.data_files().len() <= 1 || split.row_ranges().is_some() {
        return vec![split];
    }

    let deletion_files = split.data_deletion_files().map(|files| files.to_vec());
    split
        .data_files()
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, data_file)| {
            let mut builder = DataSplitBuilder::new()
                .with_snapshot(split.snapshot_id())
                .with_partition(split.partition().clone())
                .with_bucket(split.bucket())
                .with_bucket_path(split.bucket_path().to_owned())
                .with_total_buckets(split.total_buckets())
                .with_data_files(vec![data_file]);
            if let Some(deletion_file) = deletion_files
                .as_ref()
                .and_then(|files| files.get(index))
                .cloned()
            {
                builder = builder.with_data_deletion_files(vec![deletion_file]);
            }
            builder
                .build()
                .expect("single-file DataSplit must preserve source split invariants")
        })
        .collect()
}

impl DisplayAs for PaimonScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let split_count = self.partitions.len();
                let file_count: usize = self
                    .partitions
                    .iter()
                    .map(|partition| storage_files_count(partition))
                    .sum();
                let slice_ordinals = self
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| ordinal as u32)
                    .collect::<Vec<_>>();
                write!(
                    f,
                    "PaimonScanExec: slices={split_count}, slice_ordinals={slice_ordinals:?}, files={file_count}"
                )?;
                if let Some(projection) = &self.projection {
                    write!(f, ", projection={projection:?}")?;
                }
                if self.filter.is_some() {
                    write!(f, ", filter=true")?;
                }
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::scan_partitions;
    use paimon::DataSplitBuilder;
    use paimon::spec::{BinaryRow, DataFileMeta};
    use serde_json::json;

    fn data_file(name: &str) -> DataFileMeta {
        serde_json::from_value(json!({
            "_FILE_NAME": name,
            "_FILE_SIZE": 128,
            "_ROW_COUNT": 10,
            "_MIN_KEY": [],
            "_MAX_KEY": [],
            "_KEY_STATS": {
                "_MIN_VALUES": [],
                "_MAX_VALUES": [],
                "_NULL_COUNTS": []
            },
            "_VALUE_STATS": {
                "_MIN_VALUES": [],
                "_MAX_VALUES": [],
                "_NULL_COUNTS": []
            },
            "_MIN_SEQUENCE_NUMBER": 0,
            "_MAX_SEQUENCE_NUMBER": 0,
            "_SCHEMA_ID": 0,
            "_LEVEL": 0,
            "_EXTRA_FILES": [],
            "_DELETE_ROW_COUNT": null,
            "_EMBEDDED_FILE_INDEX": null
        }))
        .unwrap()
    }

    fn split(files: Vec<DataFileMeta>) -> paimon::DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_owned())
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    #[test]
    fn scan_partitions_cap_split_groups_by_target_partitions() {
        let partitions = scan_partitions(
            vec![
                split(vec![data_file("0.parquet")]),
                split(vec![data_file("1.parquet")]),
                split(vec![data_file("2.parquet")]),
                split(vec![data_file("3.parquet")]),
            ],
            2,
        );

        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[0].len(), 2);
        assert_eq!(partitions[1].len(), 2);
    }

    #[test]
    fn scan_partitions_can_split_raw_data_split_by_data_file() {
        let partitions = scan_partitions(
            vec![split(vec![
                data_file("0.parquet"),
                data_file("1.parquet"),
                data_file("2.parquet"),
            ])],
            2,
        );

        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partitions.iter().map(|splits| splits.len()).sum::<usize>(),
            3
        );
        assert!(
            partitions
                .iter()
                .flat_map(|splits| splits.iter())
                .all(|split| split.data_files().len() == 1)
        );
    }
}
