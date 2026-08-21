use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use paimon::table::Table as PaimonTable;

use super::writer::write_stream_and_commit;

#[derive(Debug)]
pub(crate) struct PaimonWriteSink {
    table: PaimonTable,
    schema: SchemaRef,
}

impl PaimonWriteSink {
    pub(crate) fn new(table: PaimonTable, schema: SchemaRef) -> Self {
        Self { table, schema }
    }
}

impl DisplayAs for PaimonWriteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PaimonWriteSink: table={}", self.table.identifier())
    }
}

#[async_trait]
impl DataSink for PaimonWriteSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        write_stream_and_commit(&self.table, data, context).await
    }
}
