use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::{StreamExt, stream};
use paimon::DataSplit;
use paimon::table::Table as PaimonTable;

use super::table_provider::datafusion_scan_error;

#[derive(Debug)]
pub(crate) struct PaimonPartitionStream {
    table: PaimonTable,
    split: DataSplit,
    schema: SchemaRef,
}

impl PaimonPartitionStream {
    pub(crate) fn new(table: PaimonTable, split: DataSplit, schema: SchemaRef) -> Self {
        Self {
            table,
            split,
            schema,
        }
    }
}

impl PartitionStream for PaimonPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let result = self
            .table
            .new_read_builder()
            .new_read()
            .map_err(datafusion_scan_error)
            .and_then(|read| {
                read.to_arrow(std::slice::from_ref(&self.split))
                    .map_err(datafusion_scan_error)
            });

        let schema = Arc::clone(&self.schema);
        match result {
            Ok(batch_stream) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                batch_stream.map(|batch| batch.map_err(datafusion_scan_error)),
            )),
            Err(error) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::once(async move { Err(error) }),
            )),
        }
    }
}
