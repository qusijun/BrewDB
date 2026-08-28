use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use paimon::table::{CommitMessage, Table as PaimonTable};

use super::table_provider::datafusion_scan_error;

const ROW_COUNT_COLUMN: &str = "row_count";
const COMMIT_MESSAGES_COLUMN: &str = "commit_messages";

#[derive(Debug)]
pub(crate) struct PaimonSinkExec {
    input: Arc<dyn ExecutionPlan>,
    table: PaimonTable,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PaimonSinkExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, table: PaimonTable) -> Self {
        let output_schema = make_commit_message_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for PaimonSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PaimonSinkExec: table={}", self.table.identifier())
    }
}

impl ExecutionPlan for PaimonSinkExec {
    fn name(&self) -> &'static str {
        "PaimonSinkExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.table.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let table = self.table.clone();
        let input = Arc::clone(&self.input);
        let stream_schema = Arc::clone(&self.output_schema);
        let stream = futures::stream::once(async move {
            write_partition(table, input, partition, context)
                .await
                .and_then(make_commit_message_batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

#[derive(Debug)]
pub(crate) struct PaimonCommitExec {
    input: Arc<dyn ExecutionPlan>,
    table: PaimonTable,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PaimonCommitExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, table: PaimonTable) -> Self {
        let output_schema = make_count_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            table,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for PaimonCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PaimonCommitExec: table={}", self.table.identifier())
    }
}

impl ExecutionPlan for PaimonCommitExec {
    fn name(&self) -> &'static str {
        "PaimonCommitExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.table.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "PaimonCommitExec invalid partition {partition}, expected 0"
            )));
        }

        let table = self.table.clone();
        let input = Arc::clone(&self.input);
        let stream_schema = Arc::clone(&self.output_schema);
        let stream = futures::stream::once(async move {
            commit_messages(table, input, context)
                .await
                .map(make_count_batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

async fn write_partition(
    table: PaimonTable,
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
) -> DataFusionResult<(u64, Vec<CommitMessage>)> {
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().map_err(datafusion_scan_error)?;
    let mut data = input.execute(partition, context)?;
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
    Ok((row_count, messages))
}

async fn commit_messages(
    table: PaimonTable,
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> DataFusionResult<u64> {
    let mut data = input.execute(0, context)?;
    let mut row_count = 0_u64;
    let mut messages = Vec::new();
    while let Some(batch) = data.next().await {
        let batch = batch?;
        let rows = batch
            .column_by_name(ROW_COUNT_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "PaimonCommitExec expected {ROW_COUNT_COLUMN} UInt64 column"
                ))
            })?;
        let encoded_messages = batch
            .column_by_name(COMMIT_MESSAGES_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "PaimonCommitExec expected {COMMIT_MESSAGES_COLUMN} Utf8 column"
                ))
            })?;

        for row in 0..batch.num_rows() {
            row_count += rows.value(row);
            messages.extend(decode_commit_messages(encoded_messages.value(row))?);
        }
    }

    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .map_err(datafusion_scan_error)?;
    Ok(row_count)
}

fn make_commit_message_batch(
    (row_count, messages): (u64, Vec<CommitMessage>),
) -> DataFusionResult<RecordBatch> {
    let row_counts = Arc::new(UInt64Array::from(vec![row_count])) as ArrayRef;
    let messages = Arc::new(StringArray::from(vec![encode_commit_messages(messages)?])) as ArrayRef;
    RecordBatch::try_new(make_commit_message_schema(), vec![row_counts, messages])
        .map_err(Into::into)
}

fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)])
        .expect("count batch schema must be valid")
}

fn make_commit_message_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        Field::new(COMMIT_MESSAGES_COLUMN, DataType::Utf8, false),
    ]))
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn encode_commit_messages(messages: Vec<CommitMessage>) -> DataFusionResult<String> {
    let messages = messages
        .into_iter()
        .map(|message| {
            serde_json::json!({
                "partition": message.partition,
                "bucket": message.bucket,
                "new_files": message.new_files,
                "new_index_files": message.new_index_files,
                "deleted_files": message.deleted_files,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&messages).map_err(|error| DataFusionError::External(Box::new(error)))
}

fn decode_commit_messages(encoded: &str) -> DataFusionResult<Vec<CommitMessage>> {
    let values = serde_json::from_str::<Vec<serde_json::Value>>(encoded)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    values
        .into_iter()
        .map(decode_commit_message)
        .collect::<DataFusionResult<Vec<_>>>()
}

fn decode_commit_message(value: serde_json::Value) -> DataFusionResult<CommitMessage> {
    let partition = required_field(&value, "partition")?;
    let bucket = required_field(&value, "bucket")?;
    let new_files = required_field(&value, "new_files")?;
    let new_index_files = required_field(&value, "new_index_files")?;
    let deleted_files = required_field(&value, "deleted_files")?;
    Ok(CommitMessage {
        partition,
        bucket,
        new_files,
        new_index_files,
        deleted_files,
    })
}

fn required_field<T: serde::de::DeserializeOwned>(
    value: &serde_json::Value,
    name: &str,
) -> DataFusionResult<T> {
    serde_json::from_value(value.get(name).cloned().ok_or_else(|| {
        DataFusionError::Execution(format!("missing Paimon commit message field: {name}"))
    })?)
    .map_err(|error| DataFusionError::External(Box::new(error)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::test::TestMemoryExec;
    use futures::StreamExt;
    use paimon::catalog::Identifier as PaimonIdentifier;
    use paimon::io::FileIO;
    use paimon::spec::{DataType as PaimonDataType, IntType, TableSchema as PaimonTableSchema};
    use paimon::table::SnapshotManager;

    use super::{PaimonCommitExec, PaimonSinkExec};

    #[test]
    fn paimon_sink_writes_partitions_in_parallel_and_commits_once() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
            let partition_0 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
            )
            .unwrap();
            let partition_1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![3]))],
            )
            .unwrap();
            let input = TestMemoryExec::try_new_exec(
                &[vec![partition_0], vec![partition_1]],
                Arc::clone(&schema),
                None,
            )
            .unwrap();
            let table = new_test_table();
            let sink = Arc::new(PaimonSinkExec::new(input, table.clone()));
            let gather = Arc::new(CoalescePartitionsExec::new(sink));
            let commit = PaimonCommitExec::new(gather, table.clone());

            let mut stream = commit.execute(0, Arc::new(TaskContext::default())).unwrap();
            let batch = stream.next().await.unwrap().unwrap();
            let count = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            assert_eq!(count.value(0), 3);
            assert!(stream.next().await.is_none());

            let snapshot_manager =
                SnapshotManager::new(table.file_io().clone(), table.location().to_string());
            assert_eq!(
                snapshot_manager.get_latest_snapshot_id().await.unwrap(),
                Some(1)
            );
        });
    }

    fn new_test_table() -> paimon::table::Table {
        let location = format!("memory:/brewdb-paimon-sink-test-{}", uuid::Uuid::new_v4());
        let file_io = FileIO::from_path(&location).unwrap().build().unwrap();
        let schema = PaimonTableSchema::new(
            0,
            &paimon::spec::Schema::builder()
                .column("id", PaimonDataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        paimon::table::Table::new(
            file_io,
            PaimonIdentifier::new("sales", "orders"),
            location,
            schema,
            None,
        )
    }
}
