//! Coordinator-worker protocol shell.

mod coordinator_to_worker;
mod worker_to_coordinator;

pub use coordinator_to_worker::{
    CoordinatorTaskEnvelope, ProtocolVersion, StagePlanRef, TaskRequestWire,
};
pub use worker_to_coordinator::{TaskProgressWire, TaskResultWire, WorkerTaskEnvelope};
