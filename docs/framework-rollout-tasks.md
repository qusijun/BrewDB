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
- runtime finalization shell
- catalog facade shell
- SQL intent entry shell
- runtime metadata shell

That means the next step is no longer "add another shell".

The next step is "close real system loops on top of the shells that now exist".

## 3. System Rollout Tasks

### Task 1: Minimal Query Closed Loop

Goal:

- make one query travel from request entry to final result return

Suggested rollout steps:

1. request entry to SQL intent
2. SQL intent to runtime admission bootstrap
3. execution graph registration to first dispatch wave
4. worker execution to task result return
5. coordinator result shaping and final query response

Protocol baseline for this task:

- SQL clients talk to BrewDB through PostgreSQL wire protocol
- protocol handling lives in `brewdb-frontend`
- `brewdb-sql` remains SQL semantics, not client wire handling

In scope:

- request admission
- query intent output
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

- insert and mutation intent families
- runtime txn context
- lock acquisition boundary
- artifact-bearing execution results
- finalization entry
- commit/abort handoff
- unknown-outcome reconciliation shell

Primary landing zones:

- `crates/brewdb-runtime/src/txns/`
- `crates/brewdb-runtime/src/locks/`
- `crates/brewdb-runtime/src/finalization/`
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

- make real table formats participate in planning and finalization without breaking upper-layer ownership

In scope:

- Lakekeeper-backed catalog resolve path
- Paimon and format adapter binding path
- scan statistics refinement
- format-aware append/rewrite planning
- commit preparation and publish contracts

Primary landing zones:

- `crates/brewdb-catalog/`
- `crates/brewdb-storage/`
- local sibling `lakekeeper` source integration

Should not define yet:

- alternate control-plane transports beyond current local and REST shapes
- all table formats at once

Expected output:

- the system loop can run against real external metadata and format boundaries

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
- `crates/brewdb-runtime/src/finalization/`
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
