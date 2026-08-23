//! Fragment transport contracts.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::execution::FragmentExecutionStatus;
use crate::execution::executor::{FragmentExecutionEnvelope, FragmentService};
use crate::runtime::errors::RpcError;
use crate::runtime::exchange::ExchangeDataPage;
use crate::storage::StorageEngine;
use uuid::Uuid;

pub trait RpcClient: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: crate::runtime::exchange::ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError>;
}

pub trait FragmentTransport: Send + Sync {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError>;

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError>;

    fn drain_exchange_pages(
        &self,
        exchange_id: crate::runtime::exchange::ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError>;
}

pub trait TransportRegistry: Send + Sync {
    fn transport(&self, endpoint: &str) -> Result<Arc<dyn FragmentTransport>, RpcError>;
}

impl TransportRegistry for BTreeMap<String, Arc<dyn FragmentTransport>> {
    fn transport(&self, endpoint: &str) -> Result<Arc<dyn FragmentTransport>, RpcError> {
        self.get(endpoint)
            .cloned()
            .ok_or_else(|| RpcError::EndpointNotFound {
                endpoint: endpoint.to_owned(),
            })
    }
}

pub struct LocalFragmentTransport {
    service: Arc<dyn FragmentService>,
}

impl Default for LocalFragmentTransport {
    fn default() -> Self {
        Self {
            service: Arc::new(crate::execution::executor::LocalFragmentExecutor::default()),
        }
    }
}

impl LocalFragmentTransport {
    pub fn new(service: Arc<dyn FragmentService>) -> Self {
        Self { service }
    }

    pub fn with_storage(storage: Arc<StorageEngine>) -> Self {
        Self::new(Arc::new(
            crate::execution::executor::LocalFragmentExecutor::with_storage(storage),
        ))
    }
}

impl FragmentTransport for LocalFragmentTransport {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.service.execute_fragment(worker_id, envelope)
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError> {
        self.service
            .send_exchange_page(page)
            .map_err(|error| RpcError::ExecutionFailed {
                reason: error.to_string(),
            })
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: crate::runtime::exchange::ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError> {
        self.service
            .drain_exchange_pages(exchange_id)
            .map_err(|error| RpcError::ExecutionFailed {
                reason: error.to_string(),
            })
    }
}

pub struct TransportRpcClient {
    transport: Arc<dyn FragmentTransport>,
}

impl TransportRpcClient {
    pub fn new(transport: Arc<dyn FragmentTransport>) -> Self {
        Self { transport }
    }
}

impl RpcClient for TransportRpcClient {
    fn execute_fragment(
        &self,
        worker_id: Uuid,
        envelope: FragmentExecutionEnvelope,
    ) -> Result<FragmentExecutionStatus, RpcError> {
        self.transport.execute_fragment(worker_id, envelope)
    }

    fn send_exchange_page(&self, page: ExchangeDataPage) -> Result<(), RpcError> {
        self.transport.send_exchange_page(page)
    }

    fn drain_exchange_pages(
        &self,
        exchange_id: crate::runtime::exchange::ExchangeId,
    ) -> Result<Vec<ExchangeDataPage>, RpcError> {
        self.transport.drain_exchange_pages(exchange_id)
    }
}
