use std::sync::Arc;

use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use paimon::table::Table as PaimonTable;

use super::table_provider::datafusion_scan_error;

pub(crate) async fn write_stream_and_commit(
    table: &PaimonTable,
    mut data: SendableRecordBatchStream,
    _context: &Arc<TaskContext>,
) -> DataFusionResult<u64> {
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().map_err(datafusion_scan_error)?;
    let mut row_count = 0_u64;
    while let Some(batch) = data.next().await {
        let batch = batch?;
        row_count += batch.num_rows() as u64;
        writer
            .write_arrow_batch(&batch)
            .await
            .map_err(datafusion_scan_error)?;
    }

    let messages = writer
        .prepare_commit()
        .await
        .map_err(datafusion_scan_error)?;
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .map_err(datafusion_scan_error)?;

    Ok(row_count)
}
