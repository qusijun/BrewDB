# BrewDB Framework Rollout Tasks

This document tracks the next rollout stage after the top-level framework shells have been scaffolded.

The key rule has changed:

- framework assembly was crate-oriented
- framework advancement from here is system-loop-oriented

The goal is to keep BrewDB moving as one coherent distributed system, not as a pile of locally complete crates.

## 1. Rollout Principles

Each rollout task should:

- close one end-to-end system loop
- touch every required architecture plane in that loop
- keep DataFusion-native planning and execution semantics intact
- avoid deepening one subsystem in isolation without proving whole-path movement

The main system planes are:

- request entry
- control plane
- planning
- execution
- storage/format
- txn/lifecycle

Horizontal infrastructure that should be wired through each task:

- logging
- error translation
- config
- metrics and observability
- request correlation
- test harness

## 2. Current Status

Already scaffolded at framework-shell level:

- runtime admission shell
- scheduler shell
- dispatcher shell
- execution graph builder shell
- execution protocol shell
- worker runtime shell
- storage planning shell
- storage statistics shell
- transaction convergence shell
- catalog service shell
- SQL statement entry shell
- runtime metadata shell

That means the next step is no longer "add another shell".

The next step is "close real system loops on top of the shells that now exist".

## 3. System Rollout Tasks

### Task 1: Minimal Query Closed Loop

Goal:

- make one query travel from request entry to final result return

Suggested rollout steps:

1. request entry to SQL statement routing
2. SQL statement routing to planner handoff
3. execution graph registration to first dispatch wave
4. worker execution to task result return
5. coordinator result shaping and final query response

Protocol baseline for this task:

- SQL clients talk to BrewDB through PostgreSQL wire protocol
- protocol handling lives in `brewdb-frontend`
- `brewdb-sql` remains SQL semantics, not client wire handling

In scope:

- request admission
- planner handoff output
- catalog resolve
- storage scan planning input
- DataFusion physical planning handoff
- `StageGraph` build
- scheduler graph admission
- worker task execution
- result return to coordinator
- final query result shell

Primary landing zones:

- `bin/brewdb/`
- `bin/brewdbd/`
- `crates/brewdb-sql/`
- `crates/brewdb-runtime/`
- `crates/brewdb-catalog/`
- `crates/brewdb-storage/`
- `crates/brewdb-execution/`

Should not define yet:

- mutation commit
- recovery after coordinator loss
- durable shuffle
- transport finalization

Expected output:

- one query path that proves the coordinator-worker control loop is real

Current Task 1 landing state as of July 29, 2026:

- `brewdbd` has a protocol-neutral server shell above frontend protocol details
- top-level launch contract is explicit:
  - `StartupQuery`
  - `AcceptOnce`
  - `AcceptLoop`
- launch output is normalized through:
  - `ServerLaunchSummary`
  - `ServerLaunchView`
  - `ServerLaunchError`
- ingress runtime is explicit rather than inferred from nullable listener state:
  - `ServerIngressRuntimeSummary::Startup`
  - `ServerIngressRuntimeSummary::Source`
  - `ServerIngressRuntimeSummary::Listener`
- listener runtime terminal states are explicit:
  - `BindFailed`
  - `AcceptFailed`
  - `AcceptOnceCompleted`
  - `AcceptLoopCompleted`
- source runtime terminal states are explicit:
  - `AcceptOnceCompleted`
  - `AcceptLoopCompleted`
  - `Failed`
- startup runtime terminal states are explicit:
  - `Completed`
  - `Failed`
- accepted connection observation now flows through:
  - accepted socket snapshot
  - connection-backed query source shell for listener accept path
  - connection context
  - opened session
  - query response
  - bootstrap/launch views
  - top-level server logs
- accept-once listener path now keeps the accepted socket attached until
  `ServerSessionLoop::drain_request_source(...)` asks for the next request,
  instead of materializing an eager one-shot query vec during accept
- connection-backed request source now has an explicit minimal state machine:
  - `PendingRead`
  - `DrainingFallback`
  - `Drained`
- accepted socket payload observation still stays protocol-neutral:
  - read raw bytes once
  - shape at most one SQL request
  - fall back to scripted requests when no payload is observed
- connection observation carries:
  - `connection_id`
  - `transport_kind`
  - `socket_origin`
  - `local_endpoint`
  - `peer_endpoint`
- startup, source, and listener failure paths now all return structured launch errors with runtime summaries and lifecycle events instead of bare strings
- frontend query closed loop is now split into two explicit coordinator-side contracts:
  - `query_service` owns runtime-bound query lifecycle and report shaping
  - `brewdb-runtime::query` owns admission, graph registration, first dispatch wave, local worker execution, and hands worker results back to frontend result shaping
- frontend query lifecycle no longer reaches directly into multiple runtime internals for counts:
  - `brewdb-runtime::query::QueryRuntimeOutcome` now exposes a stable snapshot/count view
  - frontend consumes that stable contract instead of reconstructing lifecycle numbers from scattered runtime structs
- frontend result shaping no longer consumes raw execution `TaskResult` directly:
  - `brewdb-runtime::query` exposes a stable `QueryRuntimeResultBasis`
  - frontend/result only translates that stable basis into client-facing envelopes and protocol output
- `brewdbd` top-level launch/log/bootstrap observation now reads query completion through stable server-side output accessors:
  - command tag
  - result kind
  - row count
  - without reaching through `query.client.*` fields from top-level mode-specific views
- top-level bootstrap/launch views now carry an explicit primary-query projection:
  - `primary_query`
  - `primary_connection`
  - `ingress_runtime`
  - so mode-specific logs and orchestration can depend on a stable query completion view without traversing nested ingress trees first
- top-level bootstrap/launch summaries now also expose the same primary-query/primary-connection access pattern:
  - `ServerStartupBootstrapSummary`
  - `ServerAcceptOnceBootstrapSummary`
  - `ServerBootstrapSummary`
  - `ServerLaunchSummary`
  - so orchestration code can stay on summary contracts first, and only materialize mode-specific views when it actually needs them
- `brewdbd` main bootstrap logging now prefers `ServerLaunchSummary` for shared fields:
  - runtime kind
  - lifecycle event count
  - primary connection snapshot
  - primary query command tag
  - served connection count when applicable
  - and only needs mode-specific branching for event shape, not for digging shared state out of nested views
- bootstrap launch verification is now also moving onto that same summary-first contract:
  - shared runtime/query/connection assertions should read from `ServerLaunchSummary`
  - mode-specific view types stay as projections for transport- or mode-shaped fields
  - so `ServerLaunchView` stops acting like a second shared contract beside the summary path
- launch failure logging is now moving onto the same stable error contract:
  - runtime kind
  - bind endpoint
  - lifecycle event count
  - listener bound state
  - terminal state
  - failure reason
  - so `main.rs` no longer reconstructs failure-side runtime fields ad hoc from optional runtime summaries
- server-side query output projection is now reusing the frontend's stable result snapshot instead of reshaping the same fields twice:
  - `FrontendQueryResultSnapshot`
  - server-side result kind accessor layered on top
  - so `brewdbd` query observation stays aligned with frontend result shaping and avoids a second parallel result-count/command-tag struct
- frontend query reporting is now also reusing the result layer's own stable output view instead of rewrapping it again:
  - `QueryResultOutputView`
  - `FrontendQueryLifecycleView.result`
  - so query-service no longer maintains a separate frontend-only result-count/command-tag snapshot beside result shaping
- frontend query reporting is also reusing the runtime layer's stable snapshot directly:
  - `QueryRuntimeSnapshot`
  - `FrontendQueryReport.runtime_snapshot`
  - `FrontendQueryLifecycleView.runtime`
  - so query-service no longer mirrors runtime dispatch/envelope counters into a second frontend-owned struct
- frontend query reporting is no longer storing a second summary copy inside the report object:
  - `FrontendQueryReport::summary()`
  - `FrontendQueryLifecycleView.summary`
  - so summary becomes a projection from the report's stable request/runtime/result/phase state instead of duplicated stored fields
- frontend log observation is now also composed from existing entry and summary projections instead of flattening another parallel field set:
  - `FrontendQueryLogSnapshot.entry`
  - `FrontendQueryLogSnapshot.summary`
  - so log-facing observation reuses the same request/summary facts instead of mirroring them again into one more struct layout
- runtime dispatch-to-worker handoff is now explicit at worker-group granularity instead of collapsing the whole wave into one anonymous local batch:
  - `DispatchBatch::worker_dispatch_assignments()`
  - `RunLocalDispatch.worker_dispatches`
  - `LocalDispatchOutcome.worker_dispatches`
  - `QueryRuntimeOutcome.worker_dispatches()`
  - so the phase-1 coordinator path now preserves which task slice was assigned to which worker when it crosses the dispatch/worker boundary
- query runtime and frontend request contracts now admit a worker set instead of a single hard-wired worker:
  - `RunQueryRuntime.workers`
  - `FrontendQueryRequest.workers`
  - `ClientQueryRequest.workers`
  - so the minimal query loop can already exercise multi-worker scheduling inputs even though execution still runs in-process
- coordinator runtime now also turns worker completions back into explicit scheduling facts:
  - `QueryRuntimeOutcome.completion_snapshot()`
  - `QueryRuntimeOutcome.completed_stages()`
  - `QueryRuntimeOutcome.completed_task_attempts()`
  - so task/stage completion is no longer only implicit in worker results and can become the input to later dispatch waves
- the phase-1 query plan now uses worker-driven partition count to create a minimal multi-wave graph when parallelism is greater than 1:
  - source compute stage with N partitions
  - downstream result stage released from upstream completion facts
  - so Task 1 can already prove "plan -> first dispatch wave -> completion snapshot -> later dispatch wave -> final result"
- execution progress aggregation has now been pulled under `runtime-meta` instead of being reconstructed ad hoc inside query runtime:
  - `ExecutionProgress`
  - scheduling snapshot merge
  - completed stage/task-attempt truth
  - so later coordinator services can reuse one execution-progress aggregate instead of re-deriving terminal state from raw dispatch and worker outputs each time
- runtime/frontend observation has now started consuming that execution-progress aggregate directly instead of rebuilding completion facts from scattered query-runtime fields:
  - `QueryRuntimeOutcome.execution_progress()`
  - `QueryRuntimeSnapshot.completed_stage_count`
  - `QueryRuntimeSnapshot.completed_task_attempt_count`
  - `QueryRuntimeSnapshot.published_boundary_stage_count`
  - so coordinator-facing logs and snapshots share one completion truth source
- that same execution-progress aggregate now also tracks loop shape rather than only terminal counts:
  - `QueryRuntimeSnapshot.dispatch_wave_count`
  - `QueryRuntimeSnapshot.worker_execution_round_count`
  - frontend terminal logs include the same wave/round counts
  - so Task 1 can now distinguish "one dispatch happened" from "the coordinator advanced through multiple waves"
- Task 1 has now started wiring a real query path instead of only shape contracts:
  - `brewdb-sql` uses the DataFusion parser for query-shaped SQL
  - phase-1 query planning now carries a parsed literal-select query spec into the task graph
  - worker task execution can materialize terminal output for `SELECT` literal projections like `select 1`
  - frontend/pgwire result shaping now consumes that terminal output instead of returning only `"task_succeeded"`
- Task 1 has also moved one step deeper into the real execution stack:
  - query planning now carries a DataFusion logical plan inside the phase-1 literal query path
  - worker execution builds a DataFusion physical plan from that logical plan rather than reparsing ad hoc SQL at execution time
  - so the query loop is starting to align parse -> logical plan -> physical plan -> result instead of stopping at parser output
- server query execution shell has also been thinned so frontend query reports are the direct execution return:
  - ingress adapter returns `FrontendQueryReport`
  - server result shells only add request and connection/ingress context
  - so `brewdbd` no longer inserts an extra response wrapper between frontend query completion and server-side observation
- serve-once query observation is also flattening its public access pattern:
  - `ServerServeOnceResult` now exposes direct request/report/command accessors
  - top-level summaries and tests can read query completion without stepping through `handled_query()`
  - which keeps the server-side closed-loop shell centered on one query outcome object instead of nested wrappers
- the inner `ServerQueryResult` wrapper has now been collapsed into `ServerServeOnceResult` itself:
  - request
  - frontend query report
  - optional connection snapshot
  - lifecycle events
  - so the phase-1 server query closed loop has one primary outcome object instead of separate query-result and serve-once layers
- connection-scoped serve results are also exposing direct session/query accessors:
  - opened session
  - session context
  - served queries
  - primary query
  - emitted events
  - so ingress summaries can compose from connection-level outcomes without reaching deep into nested session-result internals
- top-level serve summaries are now aligning to the same accessor pattern:
  - served connection count
  - primary connection runtime snapshot
  - primary query
  - primary query view
  - command tag
  - so bootstrap summaries can mostly delegate to serve-summary contracts instead of rebuilding top-level query/connection observations by hand

Task 1 phase-1 stable contract boundary:

- `brewdbd` may depend on a SQL ingress adapter, but should not expose `pgwire` types in its top-level runtime or launch contracts
- query execution stays Arrow-oriented below frontend/result shaping
- session/query observation should keep request identity and connection identity aligned
- launch/runtime summaries should remain stable even if the transport implementation changes later

Still intentionally out of scope for Task 1:

- real pgwire socket session state machine
- production network backpressure behavior
- durable exchange and recovery
- mutation finalization
- coordinator-loss recovery

Known validation boundary:

- crate-local formatting and selected offline tests can run in this repository
- `brewdb-catalog` should now be treated as a BrewDB-owned catalog shell rather than a Lakekeeper integration shell
- broader `brewdbd` validation should now be driven by BrewDB-local compile and test readiness instead of external catalog-service branch buildability
- current BrewDB-local validation status on July 29, 2026:
  - `cargo check -p brewdb-catalog --offline` passes
  - `cargo check -p brewdbd --offline` passes
  - `cargo test -p brewdb-frontend --offline` passes
  - `cargo test -p brewdbd --offline` passes when local TCP bind is allowed

### Task 2: Distributed Dispatch and Exchange Loop

Goal:

- make the query path genuinely MPP-shaped rather than local-only

In scope:

- full-graph admission
- dependency-driven task dispatch
- exchange-aware task dependencies
- worker-side exchange buffering
- task completion propagation
- coordinator-side release of downstream runnable tasks

Primary landing zones:

- `crates/brewdb-runtime/src/scheduler/`
- `crates/brewdb-runtime/src/dispatcher/`
- `crates/brewdb-execution/src/worker/`
- `crates/brewdb-execution/src/protocol/`

Should not define yet:

- BSP barriers as the only policy
- resumable durable exchange
- cluster elasticity logic

Expected output:

- the runtime behaves like an MPP engine while still leaving room for future BSP policies

### Task 3: Result Delivery and Observability Loop

Goal:

- make query completion visible, diagnosable, and user-facing

In scope:

- final result shaping
- result streaming or batch shell
- correlated request/job/stage/task identifiers
- structured logs across coordinator and worker paths
- user-facing error translation
- basic execution metrics hooks

Primary landing zones:

- `crates/brewdb-core/`
- `crates/brewdb-runtime/`
- `crates/brewdb-execution/`
- `bin/brewdb/`
- `bin/brewdbd/`

Should not define yet:

- full metrics backend
- production-grade tracing export
- rich client protocol negotiation

Expected output:

- a query loop that is not only runnable, but also inspectable

### Task 4: Mutation Finalization and Transaction Loop

Goal:

- extend the proven query skeleton into commit-bearing job families

In scope:

- insert and mutation statement families
- runtime txn context
- lock acquisition boundary
- artifact-bearing execution results
- transaction convergence entry
- commit/abort handoff
- unknown-outcome reconciliation shell

Primary landing zones:

- `crates/brewdb-runtime/src/txns/`
- `crates/brewdb-runtime/src/locks/`
- `crates/brewdb-runtime/src/txns/`
- `crates/brewdb-storage/src/commit/`
- `crates/brewdb-execution/src/artifacts/`

Should not define yet:

- full failure recovery algorithms
- lease rebalancing
- maintenance-policy tuning

Expected output:

- mutation jobs reuse the same main lifecycle instead of creating a side path

### Task 5: Storage-Format Deepening Loop

Goal:

- make real table formats participate in planning and transaction convergence without breaking upper-layer ownership

In scope:

- BrewDB-owned catalog resolve path
- Paimon and table-engine binding path
- scan statistics refinement
- format-aware append/rewrite planning
- commit preparation and publish contracts

Primary landing zones:

- `crates/brewdb-catalog/`
- `crates/brewdb-storage/`
- FoundationDB-backed catalog-store integration

Should not define yet:

- alternate catalog-store backends beyond the FoundationDB default
- all table formats at once

Expected output:

- the system loop can run against real BrewDB-owned metadata and format boundaries

### Task 6: Recovery and Reconciliation Loop

Goal:

- harden lifecycle truth after normal-path ownership is already clear

In scope:

- runtime metadata inspection
- commit-attempt state reconciliation
- task/job terminal-state repair entry
- restart-safe read models
- fault-injection integration tests

Primary landing zones:

- `crates/brewdb-runtime/src/jobs/`
- `crates/brewdb-runtime/src/txns/`
- `crates/brewdb-runtime/src/txns/`
- `tests/integration/`

Should not define yet:

- transparent in-flight execution resume
- durable BSP checkpointing

Expected output:

- BrewDB can explain and repair uncertain lifecycle truth instead of only succeeding on the happy path

## 4. Recommended Order

The recommended order is:

1. Task 1: Minimal Query Closed Loop
2. Task 2: Distributed Dispatch and Exchange Loop
3. Task 3: Result Delivery and Observability Loop
4. Task 4: Mutation Finalization and Transaction Loop
5. Task 5: Storage-Format Deepening Loop
6. Task 6: Recovery and Reconciliation Loop

Why this order:

- query-first proves the main control loop early
- MPP dispatch must become real before mutation complexity arrives
- observability should be attached to the first live path, not bolted on afterward
- transaction and commit semantics should extend a proven execution path
- external metadata and format truth should deepen once the control path is stable
- recovery should harden already-proven ownership boundaries rather than define them

## 5. Definition of Done

A rollout task is complete when:

- one end-to-end path is executable through the intended system boundary
- the ownership split across crates is still clear
- diagnostics and error translation follow the path
- at least one integration test proves the loop

A rollout task is not required to:

- complete every detail inside each touched crate
- freeze transport or external protocol choices too early
- solve future BSP or resumability goals ahead of need
