//! BrewDB distributed execution runtime.

pub mod catalog {
    pub use brewdb_catalog::catalog::*;
}

pub mod common {
    pub use brewdb_common::common::*;
}

pub mod planner {
    pub use brewdb_planner::planner::*;
}

pub mod parser {
    pub use brewdb_parser::{Parser, ParserError};
    pub use brewdb_parser::{ast, dialect, display_utils, keywords, tokenizer};

    pub mod parser {
        pub use brewdb_parser::parser::*;
    }
}

pub mod runtime {
    pub mod coordinator;
    pub mod driver;
    pub mod errors;
    pub mod exchange;
    pub mod exchange_service;
    pub mod execution_graph;
    pub mod fragment;
    pub mod function;
    pub mod scheduler;
    pub mod storage;
    pub mod transport;

    pub use crate::execution::{FragmentExecutionEnvelope, FragmentService};
    pub use crate::planner::LocalFragmentPlan;
    pub use coordinator::QueryCoordinator;
    pub use driver::SqlDriver;
    pub use errors::{
        ExchangeRuntimeError, ExecutionRuntimeError, FragmentSchedulerError, RpcError,
        SqlDriverError,
    };
    pub use exchange::{
        ExchangeBufferManager, ExchangeChannelDescriptor, ExchangeDataEncoding, ExchangeDataPage,
        ExchangeId, build_exchange_channels,
    };
    pub use exchange_service::{ExchangePageSink, ResultBatchSink, TransportExchangePageSink};
    pub use execution_graph::{ExecutionGraph, QueryExecutionHandle, QueryOutput};
    pub use fragment::{ExecutionFragment, FragmentInstance};
    pub use scheduler::{
        AllAtOnceFragmentScheduler, FirstWorkerSelector, FragmentScheduler, ResourceManager,
        StaticResourceManager, WorkerInfo, WorkerSelector,
    };
    pub use storage::build_storage_engine;
    pub use transport::{
        FragmentTransport, LocalFragmentTransport, RpcClient, TransportRegistry, TransportRpcClient,
    };
}

pub mod storage {
    pub use brewdb_storage::storage::*;
}

pub mod execution;

pub use execution::*;
