//! Runtime exchange channel planning.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::io::Cursor;
use std::sync::Mutex;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_select::filter::filter_record_batch;
use brewdb_planner::exchange::{ExchangeNode, ExchangeScope, ExchangeType, PartitioningScheme};
use brewdb_planner::plan::PlanFragmentId;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use uuid::Uuid;

use crate::scheduler::ScheduledFragment;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExchangeId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeChannelDescriptor {
    pub exchange_id: ExchangeId,
    pub source_fragment_id: PlanFragmentId,
    pub target_fragment_id: PlanFragmentId,
    pub source_worker_id: Uuid,
    pub source_endpoint: String,
    pub target_worker_id: Uuid,
    pub target_endpoint: String,
    pub scope: ExchangeScope,
    pub exchange_type: ExchangeType,
    pub partitioning_scheme: PartitioningScheme,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExchangeRuntimeError {
    SourceFragmentNotScheduled { fragment_id: PlanFragmentId },
    TargetFragmentNotScheduled { fragment_id: PlanFragmentId },
    ExchangeReceiverAlreadyTaken { exchange_id: ExchangeId },
    ExchangeReceiverClosed { exchange_id: ExchangeId },
    EmptyExchangeOutputs,
    InvalidExchangeRouting { reason: String },
    IpcSerializationFailed { reason: String },
    UnsupportedDataEncoding { encoding: ExchangeDataEncoding },
    BufferLockPoisoned,
}

impl fmt::Display for ExchangeRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceFragmentNotScheduled { fragment_id } => {
                write!(
                    f,
                    "exchange source fragment {:?} is not scheduled",
                    fragment_id
                )
            }
            Self::TargetFragmentNotScheduled { fragment_id } => {
                write!(
                    f,
                    "exchange target fragment {:?} is not scheduled",
                    fragment_id
                )
            }
            Self::ExchangeReceiverAlreadyTaken { exchange_id } => {
                write!(f, "exchange receiver already taken for {:?}", exchange_id)
            }
            Self::ExchangeReceiverClosed { exchange_id } => {
                write!(f, "exchange receiver is closed for {:?}", exchange_id)
            }
            Self::EmptyExchangeOutputs => write!(f, "exchange output channels are empty"),
            Self::InvalidExchangeRouting { reason } => {
                write!(f, "invalid exchange routing: {reason}")
            }
            Self::IpcSerializationFailed { reason } => {
                write!(f, "exchange Arrow IPC serialization failed: {reason}")
            }
            Self::UnsupportedDataEncoding { encoding } => {
                write!(f, "unsupported exchange data encoding: {encoding:?}")
            }
            Self::BufferLockPoisoned => write!(f, "exchange buffer lock is poisoned"),
        }
    }
}

impl Error for ExchangeRuntimeError {}

pub fn build_exchange_channels(
    exchanges: &[ExchangeNode],
    scheduled_fragments: &[ScheduledFragment],
) -> Result<Vec<ExchangeChannelDescriptor>, ExchangeRuntimeError> {
    let placements = scheduled_fragments
        .iter()
        .map(|scheduled| {
            (
                scheduled.fragment.fragment_id,
                (scheduled.worker_id, scheduled.endpoint.clone()),
            )
        })
        .collect::<HashMap<_, _>>();

    exchanges
        .iter()
        .enumerate()
        .map(|(ordinal, exchange)| {
            let (source_worker_id, source_endpoint) = placements
                .get(&exchange.source_fragment_id)
                .cloned()
                .ok_or(ExchangeRuntimeError::SourceFragmentNotScheduled {
                    fragment_id: exchange.source_fragment_id,
                })?;
            let (target_worker_id, target_endpoint) = placements
                .get(&exchange.target_fragment_id)
                .cloned()
                .ok_or(ExchangeRuntimeError::TargetFragmentNotScheduled {
                    fragment_id: exchange.target_fragment_id,
                })?;
            Ok(ExchangeChannelDescriptor {
                exchange_id: ExchangeId(ordinal as u32),
                source_fragment_id: exchange.source_fragment_id,
                target_fragment_id: exchange.target_fragment_id,
                source_worker_id,
                source_endpoint,
                target_worker_id,
                target_endpoint,
                scope: exchange.scope,
                exchange_type: exchange.exchange_type,
                partitioning_scheme: exchange.partitioning_scheme.clone(),
            })
        })
        .collect()
}

pub fn route_exchange_batch(
    channels: &[ExchangeChannelDescriptor],
    batch: RecordBatch,
) -> Result<Vec<(ExchangeChannelDescriptor, RecordBatch)>, ExchangeRuntimeError> {
    let Some(first_channel) = channels.first() else {
        return Err(ExchangeRuntimeError::EmptyExchangeOutputs);
    };
    match first_channel.exchange_type {
        ExchangeType::Gather | ExchangeType::Replicate => Ok(channels
            .iter()
            .cloned()
            .map(|channel| (channel, batch.clone()))
            .collect()),
        ExchangeType::Repartition => route_repartition_batch(channels, batch),
    }
}

fn route_repartition_batch(
    channels: &[ExchangeChannelDescriptor],
    batch: RecordBatch,
) -> Result<Vec<(ExchangeChannelDescriptor, RecordBatch)>, ExchangeRuntimeError> {
    if channels[0].partitioning_scheme.partition_keys.is_empty() {
        return Err(ExchangeRuntimeError::InvalidExchangeRouting {
            reason: "repartition exchange requires at least one partition key".to_owned(),
        });
    }

    let partition_arrays = channels[0]
        .partitioning_scheme
        .partition_keys
        .iter()
        .map(|partition_key| {
            let expr =
                PhysicalColumn::new_with_schema(&partition_key.name, batch.schema().as_ref())
                    .map_err(|err| ExchangeRuntimeError::InvalidExchangeRouting {
                        reason: err.to_string(),
                    })?;
            expr.evaluate(&batch)
                .and_then(|value| value.into_array(batch.num_rows()))
                .map_err(|err| ExchangeRuntimeError::InvalidExchangeRouting {
                    reason: err.to_string(),
                })
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;

    let mut hashes = vec![0; batch.num_rows()];
    create_hashes(&partition_arrays, &RandomState::default(), &mut hashes).map_err(|err| {
        ExchangeRuntimeError::InvalidExchangeRouting {
            reason: err.to_string(),
        }
    })?;

    let partition_count = channels.len();
    channels
        .iter()
        .enumerate()
        .filter_map(|(partition_ordinal, channel)| {
            let predicate = BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| hashes[row] as usize % partition_count == partition_ordinal)
                    .collect::<Vec<_>>(),
            );
            if predicate.true_count() == 0 {
                return None;
            }
            Some(
                filter_record_batch(&batch, &predicate)
                    .map(|filtered| (channel.clone(), filtered))
                    .map_err(|err| ExchangeRuntimeError::InvalidExchangeRouting {
                        reason: err.to_string(),
                    }),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeDataEncoding {
    ArrowIpcStream,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeDataPage {
    pub exchange_id: ExchangeId,
    pub encoding: ExchangeDataEncoding,
    pub payload: Vec<u8>,
    pub end_of_stream: bool,
}

impl ExchangeDataPage {
    pub fn from_record_batch(
        exchange_id: ExchangeId,
        batch: RecordBatch,
    ) -> Result<Self, ExchangeRuntimeError> {
        let mut payload = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut payload, batch.schema_ref()).map_err(|err| {
                    ExchangeRuntimeError::IpcSerializationFailed {
                        reason: err.to_string(),
                    }
                })?;
            writer
                .write(&batch)
                .map_err(|err| ExchangeRuntimeError::IpcSerializationFailed {
                    reason: err.to_string(),
                })?;
            writer
                .finish()
                .map_err(|err| ExchangeRuntimeError::IpcSerializationFailed {
                    reason: err.to_string(),
                })?;
        }
        Ok(Self {
            exchange_id,
            encoding: ExchangeDataEncoding::ArrowIpcStream,
            payload,
            end_of_stream: false,
        })
    }

    pub fn end_of_stream(exchange_id: ExchangeId) -> Self {
        Self {
            exchange_id,
            encoding: ExchangeDataEncoding::ArrowIpcStream,
            payload: Vec::new(),
            end_of_stream: true,
        }
    }

    pub fn into_record_batches(self) -> Result<Vec<RecordBatch>, ExchangeRuntimeError> {
        if self.end_of_stream {
            return Ok(Vec::new());
        }
        match self.encoding {
            ExchangeDataEncoding::ArrowIpcStream => {
                StreamReader::try_new(Cursor::new(self.payload), None)
                    .map_err(|err| ExchangeRuntimeError::IpcSerializationFailed {
                        reason: err.to_string(),
                    })?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|err| ExchangeRuntimeError::IpcSerializationFailed {
                        reason: err.to_string(),
                    })
            }
        }
    }
}

struct ExchangeBufferEntry {
    sender: UnboundedSender<ExchangeDataPage>,
    receiver: Option<UnboundedReceiver<ExchangeDataPage>>,
}

#[derive(Default)]
pub struct ExchangeBufferManager {
    buffers: Mutex<BTreeMap<ExchangeId, ExchangeBufferEntry>>,
}

impl std::fmt::Debug for ExchangeBufferManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExchangeBufferManager")
            .finish_non_exhaustive()
    }
}

impl ExchangeBufferManager {
    pub fn enqueue_output(
        &self,
        channel: &ExchangeChannelDescriptor,
        batch: RecordBatch,
    ) -> Result<(), ExchangeRuntimeError> {
        let page = ExchangeDataPage::from_record_batch(channel.exchange_id, batch)?;
        self.enqueue_page(page)
    }

    pub fn enqueue_page(&self, page: ExchangeDataPage) -> Result<(), ExchangeRuntimeError> {
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|_| ExchangeRuntimeError::BufferLockPoisoned)?;
        let entry = buffers.entry(page.exchange_id).or_insert_with(|| {
            let (sender, receiver) = unbounded_channel();
            ExchangeBufferEntry {
                sender,
                receiver: Some(receiver),
            }
        });
        entry
            .sender
            .send(page)
            .map_err(|err| ExchangeRuntimeError::ExchangeReceiverClosed {
                exchange_id: err.0.exchange_id,
            })?;
        Ok(())
    }

    pub fn drain_pages(
        &self,
        channel: &ExchangeChannelDescriptor,
    ) -> Result<Vec<ExchangeDataPage>, ExchangeRuntimeError> {
        self.drain_pages_by_id(channel.exchange_id)
    }

    pub fn drain_pages_by_id(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, ExchangeRuntimeError> {
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|_| ExchangeRuntimeError::BufferLockPoisoned)?;
        let Some(entry) = buffers.get_mut(&exchange_id) else {
            return Ok(vec![]);
        };
        let mut pages = Vec::new();
        if let Some(receiver) = entry.receiver.as_mut() {
            while let Ok(page) = receiver.try_recv() {
                pages.push(page);
            }
        }
        Ok(pages)
    }

    pub fn take_receiver(
        &self,
        exchange_id: ExchangeId,
    ) -> Result<UnboundedReceiver<ExchangeDataPage>, ExchangeRuntimeError> {
        let mut buffers = self
            .buffers
            .lock()
            .map_err(|_| ExchangeRuntimeError::BufferLockPoisoned)?;
        let entry = buffers.entry(exchange_id).or_insert_with(|| {
            let (sender, receiver) = unbounded_channel();
            ExchangeBufferEntry {
                sender,
                receiver: Some(receiver),
            }
        });
        entry
            .receiver
            .take()
            .ok_or(ExchangeRuntimeError::ExchangeReceiverAlreadyTaken { exchange_id })
    }

    pub fn drain_input(
        &self,
        channel: &ExchangeChannelDescriptor,
    ) -> Result<Vec<RecordBatch>, ExchangeRuntimeError> {
        self.drain_pages(channel)?
            .into_iter()
            .map(ExchangeDataPage::into_record_batches)
            .collect::<Result<Vec<_>, _>>()
            .map(|pages| pages.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use brewdb_common::runtime::QueryContext;
    use brewdb_planner::PlanStageId;
    use brewdb_planner::exchange::{ExchangeNode, PartitioningScheme};
    use brewdb_planner::plan::{PlanFragment, PlanFragmentId, PlanFragmentKind};

    use crate::scheduler::ScheduledFragment;

    use super::{
        ExchangeBufferManager, ExchangeChannelDescriptor, ExchangeDataEncoding, ExchangeDataPage,
        ExchangeId, ExchangeRuntimeError, build_exchange_channels, route_exchange_batch,
    };

    fn fragment_id(stage_id: u32) -> PlanFragmentId {
        PlanFragmentId {
            stage_id: PlanStageId(stage_id),
            fragment_ordinal: 0,
        }
    }

    fn scheduled(fragment_id: PlanFragmentId, endpoint: &str) -> ScheduledFragment {
        ScheduledFragment {
            query_context: QueryContext {
                query_id: uuid::Uuid::new_v4(),
            },
            fragment: PlanFragment {
                fragment_id,
                kind: PlanFragmentKind::Source,
                root: None,
                local_plan: None,
            },
            worker_id: uuid::Uuid::new_v4(),
            endpoint: endpoint.to_owned(),
        }
    }

    fn batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn string_batch(values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn exchange_channels_follow_scheduled_fragment_placements() {
        let source = scheduled(fragment_id(1), "rpc://worker-1");
        let target = scheduled(fragment_id(0), "rpc://worker-0");

        let channels = build_exchange_channels(
            &[ExchangeNode::gather(
                source.fragment.fragment_id,
                target.fragment.fragment_id,
            )],
            &[source.clone(), target.clone()],
        )
        .unwrap();

        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0].source_worker_id, source.worker_id);
        assert_eq!(channels[0].source_endpoint, source.endpoint);
        assert_eq!(channels[0].target_worker_id, target.worker_id);
        assert_eq!(channels[0].target_endpoint, target.endpoint);
    }

    #[test]
    fn exchange_channels_reject_unscheduled_source() {
        let target = scheduled(fragment_id(0), "rpc://worker-0");

        let err = build_exchange_channels(
            &[ExchangeNode::gather(
                fragment_id(1),
                target.fragment.fragment_id,
            )],
            &[target],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            ExchangeRuntimeError::SourceFragmentNotScheduled { .. }
        ));
    }

    #[test]
    fn exchange_buffer_manager_drains_batches_by_exchange_id() {
        let source = scheduled(fragment_id(1), "rpc://worker-1");
        let target = scheduled(fragment_id(0), "rpc://worker-0");
        let channel = build_exchange_channels(
            &[ExchangeNode::gather(
                source.fragment.fragment_id,
                target.fragment.fragment_id,
            )],
            &[source, target],
        )
        .unwrap()
        .remove(0);
        let manager = ExchangeBufferManager::default();

        manager.enqueue_output(&channel, batch(&[1, 2, 3])).unwrap();
        manager.enqueue_output(&channel, batch(&[4])).unwrap();
        let drained = manager.drain_input(&channel).unwrap();

        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].num_rows(), 3);
        assert_eq!(drained[1].num_rows(), 1);
        assert!(manager.drain_input(&channel).unwrap().is_empty());
    }

    #[test]
    fn exchange_data_page_uses_arrow_ipc_stream_payload() {
        let page = ExchangeDataPage::from_record_batch(ExchangeId(7), batch(&[1, 2, 3])).unwrap();

        assert_eq!(page.exchange_id, ExchangeId(7));
        assert_eq!(page.encoding, ExchangeDataEncoding::ArrowIpcStream);
        assert!(!page.payload.is_empty());

        let decoded = page.into_record_batches().unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
    }

    #[test]
    fn exchange_buffer_manager_buffers_arrow_ipc_pages() {
        let source = scheduled(fragment_id(1), "rpc://worker-1");
        let target = scheduled(fragment_id(0), "rpc://worker-0");
        let channel = build_exchange_channels(
            &[ExchangeNode::gather(
                source.fragment.fragment_id,
                target.fragment.fragment_id,
            )],
            &[source, target],
        )
        .unwrap()
        .remove(0);
        let manager = ExchangeBufferManager::default();

        manager.enqueue_output(&channel, batch(&[1, 2, 3])).unwrap();
        let pages = manager.drain_pages(&channel).unwrap();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].encoding, ExchangeDataEncoding::ArrowIpcStream);
        assert!(!pages[0].payload.is_empty());
    }

    #[test]
    fn exchange_batch_router_repartitions_rows_by_hash_key() {
        let source = scheduled(fragment_id(1), "rpc://worker-1");
        let target = scheduled(fragment_id(0), "rpc://worker-0");
        let mut channels = build_exchange_channels(
            &[ExchangeNode::repartition(
                source.fragment.fragment_id,
                target.fragment.fragment_id,
                PartitioningScheme::hash([datafusion_common::Column::new_unqualified("id")]),
            )],
            &[source, target],
        )
        .unwrap();
        channels.push(ExchangeChannelDescriptor {
            exchange_id: ExchangeId(42),
            ..channels[0].clone()
        });

        let routed = route_exchange_batch(&channels, batch(&[0, 1, 2, 3])).unwrap();

        assert_eq!(routed.len(), 2);
        let mut routed_values = routed
            .iter()
            .flat_map(|(_, batch)| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..values.len())
                    .map(|idx| values.value(idx))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        routed_values.sort();
        assert_eq!(routed_values, vec![0, 1, 2, 3]);
    }

    #[test]
    fn exchange_batch_router_repartition_key_is_evaluated_by_datafusion() {
        let source = scheduled(fragment_id(1), "rpc://worker-1");
        let target = scheduled(fragment_id(0), "rpc://worker-0");
        let mut channels = build_exchange_channels(
            &[ExchangeNode::repartition(
                source.fragment.fragment_id,
                target.fragment.fragment_id,
                PartitioningScheme::hash([datafusion_common::Column::new_unqualified("name")]),
            )],
            &[source, target],
        )
        .unwrap();
        channels.push(ExchangeChannelDescriptor {
            exchange_id: ExchangeId(43),
            ..channels[0].clone()
        });

        let routed = route_exchange_batch(&channels, string_batch(&["alice", "bob"])).unwrap();

        assert_eq!(
            routed
                .iter()
                .map(|(_, batch)| batch.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(routed.len(), 2);
    }
}
