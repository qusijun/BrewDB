//! Exchange sink adapters used by query coordination.

use std::sync::Arc;

use crate::runtime::exchange::{ExchangeChannelDescriptor, ExchangeDataPage};
use crate::runtime::transport::{RpcError, TransportRegistry};
use arrow::record_batch::RecordBatch;

pub trait ExchangePageSink: Send + Sync {
    fn send_page(
        &self,
        channel: &ExchangeChannelDescriptor,
        page: ExchangeDataPage,
    ) -> Result<(), RpcError>;
}

pub trait ResultBatchSink: Send + Sync {
    fn send_batch(&self, batch: RecordBatch) -> Result<(), RpcError>;
}

pub struct TransportExchangePageSink {
    transport_registry: Arc<dyn TransportRegistry>,
}

impl TransportExchangePageSink {
    pub fn new(transport_registry: Arc<dyn TransportRegistry>) -> Self {
        Self { transport_registry }
    }
}

impl ExchangePageSink for TransportExchangePageSink {
    fn send_page(
        &self,
        channel: &ExchangeChannelDescriptor,
        page: ExchangeDataPage,
    ) -> Result<(), RpcError> {
        let transport = self
            .transport_registry
            .transport(&channel.target_endpoint)?;
        transport.send_exchange_page(page)
    }
}
