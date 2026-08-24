use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DataFusionResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, stream};
use paimon::DataSplit;
use paimon::table::Table as PaimonTable;

use super::table_provider::datafusion_scan_error;

#[derive(Debug)]
pub(crate) struct PaimonScanExec {
    table: PaimonTable,
    projected_schema: SchemaRef,
    scan_splits: Vec<Arc<[DataSplit]>>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl PaimonScanExec {
    pub(crate) fn new(
        table: PaimonTable,
        projected_schema: SchemaRef,
        scan_splits: Vec<Arc<[DataSplit]>>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(scan_splits.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            projected_schema,
            scan_splits,
            projection,
            limit,
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
        let Some(splits) = self.scan_splits.get(partition).cloned() else {
            return Err(DataFusionError::Internal(format!(
                "PaimonScanExec invalid partition {partition}, expected less than {}",
                self.scan_splits.len()
            )));
        };

        let result = self
            .table
            .new_read_builder()
            .new_read()
            .map_err(datafusion_scan_error)
            .and_then(|read| read.to_arrow(&splits).map_err(datafusion_scan_error));

        let schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();
        let mut remaining = self.limit;
        match result {
            Ok(batch_stream) => Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                batch_stream.map(move |batch| {
                    let mut batch = batch.map_err(datafusion_scan_error)?;
                    if let Some(indices) = &projection {
                        batch = batch.project(indices)?;
                    }
                    if let Some(remaining_rows) = remaining.as_mut() {
                        let rows_to_take = (*remaining_rows).min(batch.num_rows());
                        *remaining_rows -= rows_to_take;
                        batch = batch.slice(0, rows_to_take);
                    }
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

impl DisplayAs for PaimonScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let split_count: usize = self.scan_splits.iter().map(|splits| splits.len()).sum();
                let file_count: usize = self
                    .scan_splits
                    .iter()
                    .flat_map(|splits| splits.iter())
                    .map(|split| split.data_files().len())
                    .sum();
                write!(
                    f,
                    "PaimonScanExec: partitions={}, splits={split_count}, files={file_count}",
                    self.scan_splits.len()
                )?;
                if let Some(projection) = &self.projection {
                    write!(f, ", projection={projection:?}")?;
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
