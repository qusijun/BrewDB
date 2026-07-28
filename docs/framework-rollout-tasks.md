# BrewDB Framework Rollout Tasks

This document breaks the remaining framework-layer work into independent tasks so the codebase can be advanced top-down without prematurely diving into implementation detail.

The scope here is framework assembly, not feature completion.

## 1. Task Breakdown Principles

Each task should:

- have one clear ownership boundary
- produce one visible top-level module or contract family
- avoid deep implementation detail
- improve main-path completeness from entry to execution to finalization

The tasks below are designed to be advanced independently, but they still have a recommended order.

## 2. Task List

### Task A: Runtime Admission Shell

Goal:

- establish the coordinator entry pipeline from request admission into planning and dispatch startup

Primary landing zone:

- `crates/brewdb-runtime/src/admission/`

Should define:

- admission command types
- admission service trait
- request-to-job bootstrap flow
- handoff into planning and dispatcher registration

Should not define yet:

- auth detail
- transport detail
- durable queueing
- retry heuristics

Expected output:

- top-level coordinator request lifecycle shell

Dependencies:

- existing `jobs`
- existing `planning`
- existing `dispatcher`

### Task B: Execution Graph Builder Shell

Goal:

- turn execution-side planning into a first-class top-level pipeline instead of a single trait file

Primary landing zone:

- `crates/brewdb-execution/src/stage_graph_builder/`
- `crates/brewdb-execution/src/fragments/`

Should define:

- fragment shell types
- build pipeline stages
- boundary detection shell
- stage split shell
- task split shell

Should not define yet:

- real DataFusion slicing logic
- cost tuning
- worker-local runtime

Expected output:

- clear execution-side graph-construction architecture

Dependencies:

- existing `plan`
- existing `boundaries`
- existing `task`

### Task C: Storage Planning and Statistics Shell

Goal:

- make `brewdb-storage` participate in optimizer and execution assembly through explicit planning contracts

Primary landing zone:

- `crates/brewdb-storage/src/statistics/`
- existing `scan/`, `append/`, `rewrite/`, `commit/`

Should define:

- scan planning input/output shell
- append planning shell
- rewrite planning shell
- commit preparation shell
- statistics provider shell

Should not define yet:

- format-specific optimization detail
- real file pruning logic
- commit publish implementation

Expected output:

- storage-facing planning and statistics boundary for upper layers

Dependencies:

- existing storage adapter model
- catalog route/model shell

### Task D: Runtime Finalization Shell

Goal:

- unify execution-complete to commit/abort/reconcile flow under one top-level framework

Primary landing zone:

- `crates/brewdb-runtime/src/finalization/`

Should define:

- finalize command types
- finalize service trait
- commit-path handoff shell
- abort-path handoff shell
- reconcile entry shell

Should not define yet:

- exact retry loops
- storage-specific publish detail
- reconciliation algorithms

Expected output:

- one clear finalization boundary instead of scattered commit/txn coordination only

Dependencies:

- existing `txns`
- existing `locks`
- existing `commit`

### Task E: Worker Runtime Shell

Goal:

- make worker responsibilities explicit as a top-level framework, not only implied by task contracts

Primary landing zone:

- `crates/brewdb-execution/src/worker/`

Should define:

- task executor shell
- artifact writer shell
- local data plane shell
- task report path shell

Should not define yet:

- actual execution engine logic
- spill internals
- transport server details

Expected output:

- explicit worker runtime architecture

Dependencies:

- existing `task`
- existing `artifacts`

### Task F: Catalog Facade Completion Shell

Goal:

- make catalog access a stable upper-layer facade instead of a loose set of modules

Primary landing zone:

- existing `crates/brewdb-catalog/src/facade.rs`
- supporting `cache/`, `client/`, `normalize/`

Should define:

- resolve table shell
- resolve warehouse/profile shell
- route lookup shell
- normalized metadata access shell

Should not define yet:

- concrete Lakekeeper client behavior
- cache invalidation policy detail

Expected output:

- stable catalog control-plane entry boundary

Dependencies:

- existing catalog model and route shells

### Task G: SQL Intent Entry Shell

Goal:

- turn SQL modules into one explicit frontend-to-intent pipeline

Primary landing zone:

- `crates/brewdb-sql/src/intent/`
- optional `frontend/` or `planner_entry/`

Should define:

- statement-to-intent entry trait or service
- query / insert / mutation / maintenance / DDL intent families
- capability gate shell

Should not define yet:

- parser detail rewrites
- optimizer detail
- execution planning detail

Expected output:

- one clean SQL frontend output boundary

Dependencies:

- existing parse/bind/analyze/rewrite modules

### Task H: Execution Protocol Shell

Goal:

- reserve a clear transport boundary between coordinator and worker without freezing RPC too early

Primary landing zone:

- `crates/brewdb-execution/src/protocol/`

Should define:

- coordinator-to-worker DTO shell
- worker-to-coordinator DTO shell
- fragment wire shell
- task result wire shell

Should not define yet:

- gRPC/HTTP choice
- transport encoding finalization
- version-negotiation detail

Expected output:

- stable internal protocol boundary

Dependencies:

- existing `task`
- future `fragments`

## 3. Recommended Order

The independent tasks should still be advanced in this order:

1. Task A: Runtime Admission Shell
2. Task B: Execution Graph Builder Shell
3. Task C: Storage Planning and Statistics Shell
4. Task D: Runtime Finalization Shell
5. Task E: Worker Runtime Shell
6. Task F: Catalog Facade Completion Shell
7. Task G: SQL Intent Entry Shell
8. Task H: Execution Protocol Shell

Rationale:

- A closes the coordinator entry gap
- B closes the execution graph construction gap
- C gives optimizer and execution real upstream planning contracts
- D closes the post-execution lifecycle gap
- E makes the worker side explicit
- F and G stabilize upper-layer entry boundaries
- H is best done after task and fragment shells are clearer

## 4. Definition of Done Per Task

A task is framework-complete when:

- the top-level module exists
- service or trait boundaries are explicit
- command/input/output types are explicit
- ownership boundaries are consistent with existing architecture docs
- workspace compiles

A task is not required to:

- contain full logic
- connect to external systems
- finalize storage- or transport-specific behavior

## 5. Current Status

Already scaffolded before this task breakdown:

- planning shell
- scheduler shell
- dispatcher shell
- stage graph shell
- task contract shell
- runtime metadata shell
- runtime admission shell

Immediate next task:

- Task B: Execution Graph Builder Shell
