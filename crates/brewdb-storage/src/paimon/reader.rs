use std::sync::Arc;
use std::task::{Context, Poll};

use crate::storage::TableScanSplit;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, stream};
use paimon::spec::Predicate;
use paimon::table::Table as PaimonTable;
use paimon::{DataSplit, DataSplitBuilder};

use super::table_provider::datafusion_scan_error;

#[derive(Debug)]
pub(crate) struct PaimonNativeScanExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl PaimonNativeScanExec {
    pub(crate) fn try_new(
        schema: SchemaRef,
        scan_splits: &[TableScanSplit],
        projection: Option<&Vec<usize>>,
        target_partitions: usize,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let inner =
            Self::build_parquet_exec(schema, scan_splits, projection, target_partitions, limit)?;
        Ok(Arc::new(Self { inner }) as Arc<dyn ExecutionPlan>)
    }

    fn build_parquet_exec(
        schema: SchemaRef,
        scan_splits: &[TableScanSplit],
        projection: Option<&Vec<usize>>,
        target_partitions: usize,
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut source = ParquetSource::new(Arc::clone(&schema));
        source = source
            .with_pushdown_filters(true)
            .with_reorder_filters(true);

        let file_groups = build_file_groups(scan_splits, target_partitions);
        let mut builder =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                .with_file_groups(file_groups)
                .with_limit(limit);
        if let Some(projection) = projection.cloned() {
            builder = builder.with_projection_indices(Some(projection))?;
        }
        Ok(DataSourceExec::from_data_source(builder.build()))
    }
}

impl ExecutionPlan for PaimonNativeScanExec {
    fn name(&self) -> &str {
        "PaimonNativeScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &datafusion::physical_plan::projection::ProjectionExec,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        self.inner
            .clone()
            .try_swapping_with_projection(projection)
            .map(|maybe_plan| {
                maybe_plan.map(|inner| Arc::new(Self { inner }) as Arc<dyn ExecutionPlan>)
            })
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &datafusion_common::config::ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        self.inner
            .handle_child_pushdown_result(phase, child_pushdown_result, config)
            .map(|mut propagation| {
                propagation.updated_node = propagation
                    .updated_node
                    .map(|inner| Arc::new(Self { inner }) as Arc<dyn ExecutionPlan>);
                propagation
            })
    }

    fn try_pushdown_sort(
        &self,
        order: &[datafusion::physical_expr_common::sort_expr::PhysicalSortExpr],
    ) -> DataFusionResult<datafusion::physical_plan::SortOrderPushdownResult<Arc<dyn ExecutionPlan>>>
    {
        self.inner.clone().try_pushdown_sort(order).map(|result| {
            result.try_map(|inner| {
                Ok::<_, datafusion_common::DataFusionError>(
                    Arc::new(Self { inner }) as Arc<dyn ExecutionPlan>
                )
            })
        })?
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.clone().with_new_children(children)?;
        Ok(Arc::new(Self { inner }) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

impl DisplayAs for PaimonNativeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PaimonNativeScanExec: ")?;
        self.inner.fmt_as(t, f)
    }
}
#[derive(Debug)]
pub(crate) struct PaimonScanExec {
    table: PaimonTable,
    projected_schema: SchemaRef,
    partitions: Vec<Arc<[DataSplit]>>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
    limit: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
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
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        match result {
            Ok(batch_stream) => {
                let input = Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&schema),
                    batch_stream.map(move |batch| {
                        let mut batch = batch.map_err(datafusion_scan_error)?;
                        if let Some(remaining_rows) = remaining.as_mut() {
                            let rows_to_take = (*remaining_rows).min(batch.num_rows());
                            *remaining_rows -= rows_to_take;
                            batch = batch.slice(0, rows_to_take);
                        }
                        Ok(batch)
                    }),
                ));
                Ok(Box::pin(PaimonScanStream {
                    input,
                    baseline_metrics,
                }))
            }
            Err(error) => {
                let input = Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::once(async move { Err(error) }),
                ));
                Ok(Box::pin(PaimonScanStream {
                    input,
                    baseline_metrics,
                }))
            }
        }
    }
}

struct PaimonScanStream {
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for PaimonScanStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for PaimonScanStream {
    type Item = DataFusionResult<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        let poll = self.input.as_mut().poll_next(cx);
        self.baseline_metrics.record_poll(poll)
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

fn build_file_groups(scan_splits: &[TableScanSplit], target_partitions: usize) -> Vec<FileGroup> {
    let files = scan_splits
        .iter()
        .flat_map(|scan_split| scan_split.data_files.iter())
        .map(|file| PartitionedFile::new(file.path.clone(), file.file_size.unwrap_or_default()))
        .collect::<Vec<_>>();
    FileGroup::new(files).split_files(target_partitions.max(1))
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
