# Fragment Instance Worker Prepare Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move fragment preparation from the coordinator to the worker while merging `FragmentTask` into the scheduled `FragmentInstance` boundary.

**Architecture:** `FragmentInstance` remains the scheduled execution unit and continues to hold `ExecutionFragment`, preserving the execution graph layering. Transport sends `FragmentInstance` to the worker; worker-side `LocalFragmentExecutor` prepares a `LocalFragmentPlan` using its process-local `StorageEngine` singleton. `ExchangePageSink` and `ResultBatchSink` stay outside the codec-friendly instance descriptor in an execution envelope owned by the local transport/runtime layer.

**Tech Stack:** Rust, DataFusion logical plans, BrewDB runtime scheduler/transport, existing `StorageEngine` and `LocalFragmentPlan` rewrite rules.

---

## File Structure

- `crates/brewdb/src/runtime/execution_graph.rs`
  - Keep `FragmentInstance` as the scheduled unit.
  - Keep `execution_fragment: Box<ExecutionFragment>`.
  - Ensure it carries all worker-preparation inputs: `query_context`, `table_catalogs`, `table_scan_splits`, `exchange_inputs`, and `exchange_outputs`.
- `crates/brewdb/src/runtime/rpc.rs`
  - Remove `FragmentTask`.
  - Add an internal `FragmentExecutionEnvelope` for non-codec runtime attachments: `FragmentInstance`, optional `ExchangePageSink`, optional `ResultBatchSink`.
  - Change `FragmentTransport`, `RpcClient`, and `FragmentService` execution APIs to accept `FragmentExecutionEnvelope` or an equivalent local-only wrapper where sinks are needed.
  - Add `StorageEngine` ownership to `LocalFragmentExecutor`.
  - Prepare `LocalFragmentPlan` inside `LocalFragmentExecutor::execute_fragment`.
- `crates/brewdb/src/runtime/coordinator.rs`
  - Stop calling `prepare_fragment` before transport dispatch.
  - Build and send `FragmentInstance` plus local sink envelope.
  - Keep coordinator scheduling and exchange channel attachment unchanged.
- Tests stay in existing modules:
  - `crates/brewdb/src/runtime/rpc.rs`
  - `crates/brewdb/src/runtime/coordinator.rs`
  - `crates/brewdb/src/runtime/execution_graph.rs`

---

### Task 1: Make Worker Execution Payload Preserve FragmentInstance

**Files:**
- Modify: `crates/brewdb/src/runtime/rpc.rs`
- Test: `crates/brewdb/src/runtime/rpc.rs`

- [ ] **Step 1: Write the failing test**

Add this test to the `#[cfg(test)] mod tests` in `crates/brewdb/src/runtime/rpc.rs`. If a helper with the same purpose already exists in the module, reuse the existing helper names and only add the assertion logic.

```rust
#[test]
fn fragment_execution_envelope_keeps_sinks_out_of_fragment_instance() {
    let instance = FragmentInstance::scheduled(
        uuid::Uuid::new_v4(),
        ExecutionFragment::new(PlanFragment {
            fragment_id: PlanFragmentId(7),
            kind: PlanFragmentKind::Source,
            root: None,
            local_plan: Some(DataFusionLogicalPlan::EmptyRelation(
                datafusion_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: std::sync::Arc::new(
                        datafusion_common::DFSchema::empty(),
                    ),
                },
            )),
        }),
        uuid::Uuid::new_v4(),
        "rpc://worker-7",
        TableScanSplitGroup::default(),
    );

    let envelope = FragmentExecutionEnvelope::new(instance.clone());

    assert_eq!(envelope.instance.fragment_id(), PlanFragmentId(7));
    assert!(envelope.exchange_page_sink.is_none());
    assert!(envelope.result_batch_sink.is_none());
    assert_eq!(instance.fragment_id(), PlanFragmentId(7));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test fragment_execution_envelope_keeps_sinks_out_of_fragment_instance
```

Expected: FAIL because `FragmentExecutionEnvelope` does not exist yet.

- [ ] **Step 3: Add the execution envelope**

In `crates/brewdb/src/runtime/rpc.rs`, remove the `FragmentTask` struct after downstream call sites are updated in later tasks. For this task, add the envelope beside the current task type so the test can compile:

```rust
#[derive(Clone)]
pub struct FragmentExecutionEnvelope {
    pub instance: FragmentInstance,
    pub exchange_page_sink: Option<Arc<dyn ExchangePageSink>>,
    pub result_batch_sink: Option<Arc<dyn ResultBatchSink>>,
}

impl FragmentExecutionEnvelope {
    pub fn new(instance: FragmentInstance) -> Self {
        Self {
            instance,
            exchange_page_sink: None,
            result_batch_sink: None,
        }
    }

    pub fn with_exchange_page_sink(
        mut self,
        exchange_page_sink: Arc<dyn ExchangePageSink>,
    ) -> Self {
        self.exchange_page_sink = Some(exchange_page_sink);
        self
    }

    pub fn with_result_batch_sink(mut self, result_batch_sink: Arc<dyn ResultBatchSink>) -> Self {
        self.result_batch_sink = Some(result_batch_sink);
        self
    }
}
```

Also add this import near the top of `rpc.rs`:

```rust
use crate::runtime::execution_graph::FragmentInstance;
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
cargo test fragment_execution_envelope_keeps_sinks_out_of_fragment_instance
```

Expected: PASS.

---

### Task 2: Change Transport Contracts From FragmentTask to Envelope

**Files:**
- Modify: `crates/brewdb/src/runtime/rpc.rs`
- Modify: `crates/brewdb/src/runtime/coordinator.rs`
- Test: `crates/brewdb/src/runtime/rpc.rs`

- [ ] **Step 1: Write the failing test**

Update the existing `local_fragment_transport_forwards_to_service` test in `crates/brewdb/src/runtime/rpc.rs` so the recording service receives a `FragmentExecutionEnvelope` and asserts the forwarded instance id. Use this assertion in the test body:

```rust
assert_eq!(
    service
        .received_instances
        .lock()
        .expect("received instance log lock must not be poisoned")[0],
    instance.instance_id
);
```

The recording service should store `Vec<uuid::Uuid>`:

```rust
#[derive(Default)]
struct RecordingFragmentService {
    received_instances: std::sync::Mutex<Vec<uuid::Uuid>>,
}
```

Its `execute_fragment` method should push `envelope.instance.instance_id`.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test local_fragment_transport_forwards_to_service
```

Expected: FAIL because the transport and service traits still use `FragmentTask`.

- [ ] **Step 3: Update RPC traits and local transport**

In `crates/brewdb/src/runtime/rpc.rs`, change these signatures:

```rust
fn execute_fragment(
    &self,
    worker_id: Uuid,
    envelope: FragmentExecutionEnvelope,
) -> Result<FragmentExecutionStatus, RpcError>;
```

Apply the same signature to:

- `RpcClient`
- `FragmentTransport`
- `FragmentService`
- `LocalFragmentTransport`
- the `RpcClient` implementation for transport-backed clients
- test recording services/transports

Remove `FragmentTask` only after all references are gone.

- [ ] **Step 4: Update coordinator dispatch to create an envelope**

In `crates/brewdb/src/runtime/coordinator.rs`, replace the `FragmentTask::new(LocalFragmentPlan { ... })` construction with:

```rust
let mut envelope = FragmentExecutionEnvelope::new(instance)
    .with_exchange_page_sink(page_sink);
if is_root_fragment && returns_rows {
    envelope = envelope.with_result_batch_sink(result_batch_sink);
}
client.execute_fragment(worker_id, envelope).map_err(|err| {
    ExecutionRuntimeError::InvalidPlan {
        reason: err.to_string(),
    }
})?;
```

Before this snippet, compute `is_root_fragment` from `instance.fragment().kind` before moving `instance` into the envelope.

- [ ] **Step 5: Run the focused transport test**

Run:

```bash
cargo test local_fragment_transport_forwards_to_service
```

Expected: PASS.

---

### Task 3: Move LocalFragmentPlan::prepare Into LocalFragmentExecutor

**Files:**
- Modify: `crates/brewdb/src/runtime/rpc.rs`
- Modify: `crates/brewdb/src/runtime/coordinator.rs`
- Test: `crates/brewdb/src/runtime/rpc.rs`

- [ ] **Step 1: Write the failing test**

Add a worker-side prepare test to `crates/brewdb/src/runtime/rpc.rs`:

```rust
#[test]
fn local_fragment_executor_prepares_fragment_instance_on_worker() {
    let query_context = QueryContext {
        query_id: uuid::Uuid::new_v4(),
    };
    let logical_plan = DataFusionLogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: false,
        schema: std::sync::Arc::new(datafusion_common::DFSchema::empty()),
    });
    let fragment = PlanFragment {
        fragment_id: PlanFragmentId(3),
        kind: PlanFragmentKind::Root,
        root: None,
        local_plan: Some(logical_plan),
    };
    let mut instance = FragmentInstance::scheduled(
        uuid::Uuid::new_v4(),
        ExecutionFragment::new(fragment),
        uuid::Uuid::new_v4(),
        "rpc://worker-3",
        TableScanSplitGroup::default(),
    );
    instance.query_context = query_context.clone();

    let status = LocalFragmentExecutor::default()
        .execute_fragment(uuid::Uuid::new_v4(), FragmentExecutionEnvelope::new(instance))
        .unwrap();

    assert_eq!(status.query_context, query_context);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test local_fragment_executor_prepares_fragment_instance_on_worker
```

Expected: FAIL because `LocalFragmentExecutor` still expects a prebuilt local plan inside `FragmentTask`.

- [ ] **Step 3: Add worker-local storage to LocalFragmentExecutor**

In `crates/brewdb/src/runtime/rpc.rs`, add:

```rust
use crate::storage::StorageEngine;
```

Change the struct:

```rust
pub struct LocalFragmentExecutor {
    exchange_buffers: Arc<ExchangeBufferManager>,
    session: SessionContext,
    storage: Arc<dyn StorageEngine>,
    tokio_runtime: OnceLock<Runtime>,
}
```

Update constructors:

```rust
impl Default for LocalFragmentExecutor {
    fn default() -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            session: open_session(),
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }
}

impl LocalFragmentExecutor {
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            exchange_buffers: Arc::new(ExchangeBufferManager::default()),
            session: open_session(),
            storage,
            tokio_runtime: OnceLock::new(),
        }
    }

    pub fn with_exchange_buffer_manager(exchange_buffers: Arc<ExchangeBufferManager>) -> Self {
        Self {
            exchange_buffers,
            session: open_session(),
            storage: crate::runtime::storage::build_storage_engine(),
            tokio_runtime: OnceLock::new(),
        }
    }
}
```

- [ ] **Step 4: Prepare inside execute_fragment**

Replace the `LocalFragmentExecutor` `execute_fragment` implementation body with:

```rust
let FragmentExecutionEnvelope {
    instance,
    exchange_page_sink,
    result_batch_sink,
} = envelope;
let prepared = LocalFragmentPlan::prepare(
    instance.query_context.clone(),
    instance.execution_fragment.fragment,
    instance.table_catalogs,
    instance.table_scan_splits,
    Arc::clone(&self.storage),
)
.map_err(|err| RpcError::ExecutionFailed {
    reason: err.to_string(),
})?;
let envelope = FragmentExecutionEnvelope {
    instance: FragmentInstance {
        query_context: prepared.query_context.clone(),
        table_scan_splits: prepared.table_scan_splits.clone(),
        execution_fragment: Box::new(ExecutionFragment::new(PlanFragment {
            fragment_id: prepared.fragment_id,
            kind: prepared.fragment_kind.clone(),
            root: None,
            local_plan: Some(prepared.logical_plan.clone()),
        })),
        exchange_inputs: instance.exchange_inputs,
        exchange_outputs: instance.exchange_outputs,
        table_catalogs: Vec::new(),
        ..instance
    },
    exchange_page_sink,
    result_batch_sink,
};
let logical_plan = self.materialize_exchange_inputs(
    prepared.logical_plan,
    envelope.instance.exchange_inputs.as_slice(),
)?;
self.execute_streaming(prepared.query_context.clone(), logical_plan, &envelope)?;
Ok(FragmentExecutionStatus {
    query_context: prepared.query_context,
})
```

Then change `execute_streaming` to accept `&FragmentExecutionEnvelope` and read `exchange_outputs`, `exchange_page_sink`, and `result_batch_sink` from the envelope.

Change `materialize_exchange_inputs` to accept `exchange_inputs: &[ExchangeChannelDescriptor]` instead of a task reference.

- [ ] **Step 5: Run the focused worker prepare test**

Run:

```bash
cargo test local_fragment_executor_prepares_fragment_instance_on_worker
```

Expected: PASS.

---

### Task 4: Remove Coordinator-Side Preparation

**Files:**
- Modify: `crates/brewdb/src/runtime/coordinator.rs`
- Modify: `crates/brewdb/src/runtime/execution_graph.rs`
- Test: `crates/brewdb/src/runtime/execution_graph.rs`

- [ ] **Step 1: Write the failing test**

Add this test to `crates/brewdb/src/runtime/execution_graph.rs`:

```rust
#[test]
fn runtime_dispatches_fragment_instance_without_coordinator_prepare() {
    let fragment = build_fragment();
    let query_context = QueryContext {
        query_id: uuid::Uuid::new_v4(),
    };
    let request = QueryExecutionRequest {
        query_context: query_context.clone(),
        distributed_plan: DistributedFragmentPlan {
            query_context: query_context.clone(),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: "SELECT".to_owned(),
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        },
    };

    let handle = QueryCoordinator::default().execute_query(request).unwrap();

    assert_eq!(handle.query_context, query_context);
}
```

This test should continue to use the public coordinator execution path.

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test runtime_dispatches_fragment_instance_without_coordinator_prepare
```

Expected: FAIL until coordinator dispatch no longer constructs `LocalFragmentPlan`.

- [ ] **Step 3: Delete coordinator prepare from dispatch**

In `execute_fragment_instances`, remove:

```rust
let prepared = self.prepare_fragment(
    query_context,
    fragment,
    table_scan_splits,
    table_catalogs,
)?;
```

Build the envelope from the original `FragmentInstance`.

Keep `prepare_local_fragment` available for focused tests if still useful, but remove private `prepare_fragment` if it becomes unused.

- [ ] **Step 4: Run the dispatch test**

Run:

```bash
cargo test runtime_dispatches_fragment_instance_without_coordinator_prepare
```

Expected: PASS.

---

### Task 5: Wire Single-Node Fast Path to the Same Storage Singleton

**Files:**
- Modify: `crates/brewdb/src/runtime/coordinator.rs`
- Modify: `crates/brewdb/src/runtime/rpc.rs`
- Test: `crates/brewdb/src/runtime/execution_graph.rs`

- [ ] **Step 1: Write the failing test**

Add this assertion to the existing single-node fast path test `runtime_executes_query_through_single_node_fast_path` or add a new adjacent test:

```rust
#[test]
fn single_node_fast_path_uses_worker_side_prepare_path() {
    let fragment = build_fragment();
    let query_context = QueryContext {
        query_id: uuid::Uuid::new_v4(),
    };
    let request = QueryExecutionRequest {
        query_context: query_context.clone(),
        distributed_plan: DistributedFragmentPlan {
            query_context: query_context.clone(),
            root: DistributedPlanRoot::Fragments,
            table_catalogs: vec![],
            command_tag: "SELECT".to_owned(),
            returns_rows: true,
            fragments: vec![fragment],
            fragment_scan_splits: vec![],
            exchanges: vec![],
        },
    };

    let handle = QueryCoordinator::default()
        .execute_query(request)
        .unwrap();

    assert_eq!(handle.command_tag, "SELECT");
    assert!(handle.returns_rows);
}
```

- [ ] **Step 2: Run the test to verify it fails if default transport still owns an unrelated storage**

Run:

```bash
cargo test single_node_fast_path_uses_worker_side_prepare_path
```

Expected: FAIL if constructor signatures are inconsistent, or PASS if the previous task already made the default worker path valid. If it passes immediately, keep it as a regression test.

- [ ] **Step 3: Add storage-aware local transport constructor**

In `crates/brewdb/src/runtime/rpc.rs`, add:

```rust
impl LocalFragmentTransport {
    pub fn with_storage(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            service: Arc::new(LocalFragmentExecutor::with_storage(storage)),
        }
    }
}
```

- [ ] **Step 4: Use the same singleton for QueryCoordinator defaults**

In `QueryCoordinator::default`, build storage once and pass it to both coordinator and default local transport:

```rust
let storage = crate::runtime::storage::build_storage_engine();
Self {
    scheduler: AllAtOnceFragmentScheduler::default(),
    resource_manager: Arc::new(StaticResourceManager::new(vec![WorkerInfo {
        worker_id: uuid::Uuid::nil(),
        endpoint: "rpc://worker-0".to_owned(),
    }])),
    transport_registry: Arc::new(std::collections::BTreeMap::from([(
        "rpc://worker-0".to_owned(),
        Arc::new(LocalFragmentTransport::with_storage(Arc::clone(&storage)))
            as Arc<dyn crate::runtime::rpc::FragmentTransport>,
    )])),
    storage,
    catalog_service: None,
}
```

- [ ] **Step 5: Run the single-node test**

Run:

```bash
cargo test single_node_fast_path_uses_worker_side_prepare_path
```

Expected: PASS.

---

### Task 6: Final Cleanup and Verification

**Files:**
- Modify: `crates/brewdb/src/runtime/rpc.rs`
- Modify: `crates/brewdb/src/runtime/coordinator.rs`
- Modify: `crates/brewdb/src/runtime/execution_graph.rs`

- [ ] **Step 1: Remove obsolete imports and types**

Remove all remaining imports of `FragmentTask`. Remove coordinator-side construction of `LocalFragmentPlan` for dispatch. Keep `prepare_local_fragment` only if tests or public callers still use it.

- [ ] **Step 2: Search for stale references**

Run:

```bash
rg -n "FragmentTask|prepare_fragment\\(|LocalFragmentPlan \\{" crates/brewdb/src/runtime crates/brewdb/src/planner
```

Expected: no `FragmentTask`; no coordinator dispatch `prepare_fragment`; `LocalFragmentPlan` construction remains inside `planner/local` and tests only.

- [ ] **Step 3: Format**

Run:

```bash
cargo fmt
```

Expected: exit code 0.

- [ ] **Step 4: Run full tests**

Run:

```bash
cargo test
```

Expected: all unit tests and doc-tests pass.

---

## Self-Review

- Spec coverage: The plan preserves `ExecutionFragment` inside `FragmentInstance`, moves worker-local prepare into `LocalFragmentExecutor`, keeps sinks outside the codec-friendly instance via `FragmentExecutionEnvelope`, and keeps single-node fast path on the same semantic path as distributed execution.
- Placeholder scan: The plan contains no TBD/TODO placeholders. Each task has concrete file paths, code snippets, commands, and expected outcomes.
- Type consistency: `FragmentExecutionEnvelope`, `FragmentInstance`, `ExecutionFragment`, `LocalFragmentExecutor`, `LocalFragmentPlan`, `ExchangePageSink`, and `ResultBatchSink` are named consistently across tasks.
