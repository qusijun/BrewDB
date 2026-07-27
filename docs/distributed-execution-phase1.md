# BrewDB Distributed Execution Phase 1

This document defines the Phase 1 distributed execution framework for BrewDB. It covers stage boundaries, task behavior, worker runtime, coordinator-worker task contracts, and execution outputs. It does not define long-task resumability or BSP-style recovery.

## 1. Scope

Phase 1 execution supports:

- distributed query execution
- append-like mutation execution
- rewrite-like mutation execution
- maintenance-oriented selection and rewrite execution
- staged artifact production for commit-oriented jobs

Phase 1 does not support:

- resumable long-running execution after coordinator loss
- BSP-style checkpoint/restart
- durable shuffle as a recovery contract
- transparent in-flight job migration

## 2. Execution Role

The execution framework is responsible for:

- slicing physical plans into stages
- slicing stages into tasks
- worker assignment and execution
- exchange and shuffle
- materialization of staged outputs
- delivery of boundary results back to the control plane

It is not responsible for:

- job lifecycle ownership
- table leases
- transaction state
- commit journaling
- final metadata publish

Execution computes candidate results. It does not make them externally visible.

## 3. Stage Boundary Types

Phase 1 uses three boundary types.

### `ExchangeBoundary`

Used for:

- repartition
- shuffle
- distributed sort or aggregation phase changes

### `MaterializationBoundary`

Used when execution must persist staged outputs, including:

- staged append data
- staged mutation effect artifacts
- staged rewrite outputs

### `SelectionBoundary`

Used when execution must first stabilize an intermediate selection result, including:

- compact candidate-set selection
- future rewrite-scope selection

## 4. Stages and Tasks

### `Stage`

A stage is a distributed execution fragment terminated by a defined boundary.

Useful stage kinds in Phase 1 are:

- `compute`
- `exchange`
- `materialize`
- `selection`

### `Task`

A task is the smallest scheduled execution unit within a stage.

### `TaskAttempt`

A task attempt is one concrete worker execution of a task.

Task attempts may return:

- execution progress only
- artifact-bearing outputs

## 5. Execution Skeleton

All Phase 1 job types follow the same high-level shape:

1. build an execution plan
2. slice it into stages
3. run tasks on workers
4. cross exchange/materialization/selection boundaries
5. aggregate stage outputs
6. hand the resulting artifacts or boundary results back to the control plane

Different job families have different terminal outputs:

- query -> result stream
- append-like mutation -> append artifact bundle inputs
- rewrite-like mutation -> mutation artifact bundle inputs
- maintenance selection -> candidate-set outputs
- maintenance rewrite -> rewrite artifact bundle inputs

## 6. Mutation-Aware Execution

### Append-like mutation

Execution is source-oriented:

1. produce new rows
2. normalize to target schema
3. repartition/sort/cluster as needed
4. materialize staged append artifacts

### Rewrite-like mutation

Execution is target-aware:

1. scan target rows
2. match the target row-set
3. apply row transforms
4. materialize mutation effects
5. produce staged rewrite/delete/replacement artifacts

### Maintenance execution

Maintenance jobs may use a selection boundary before rewrite:

1. identify candidate scope
2. materialize candidate-set results
3. run rewrite stages if needed
4. materialize staged rewrite artifacts

## 7. Worker Responsibilities

Workers are responsible for:

- executing DataFusion-aligned physical fragments
- local shuffle and spill
- staged artifact writing
- task result packaging
- boundary result reporting

Workers are not responsible for:

- final commit
- lease acquisition
- transaction state updates
- catalog mutation

Workers are execution engines, not control-plane owners.

## 8. Worker Runtime

Phase 1 workers are enhanced executors rather than passive fragment runners.

They may understand:

- fragment execution
- local exchange and spill
- staged artifact writing
- task-local result shaping
- boundary-aware output contracts

They do not own:

- job truth
- transaction truth
- lease truth
- catalog truth

Useful internal worker responsibilities are:

- `TaskExecutor` for fragment execution
- `LocalDataPlane` for in-memory flow, shuffle, buffering, and spill
- `ArtifactWriter` for staged artifact persistence
- `TaskResultBuilder` for packaging task outputs
- `ReportPath` for task state and completion reporting

Worker-local state is disposable. Durable truth remains in runtime metadata and external format state.

## 9. Coordinator-Worker Task Contract

The task contract is the execution framework's minimum distributed protocol.

### `TaskRequest`

A task request should carry:

- identity: `job_id`, `stage_id`, `task_id`, `attempt_id`
- execution fragment
- input partition or shard assignment
- upstream dependency references when needed
- stage boundary kind
- runtime context
- task role metadata

Useful task roles in Phase 1 include:

- `compute`
- `exchange_producer`
- `exchange_consumer`
- `append_materialize`
- `rewrite_materialize`
- `selection_materialize`

### `TaskResult`

A task result should carry:

- identity echo
- completion status
- lightweight execution summary
- boundary outputs
- failure information when needed

Task results may report either:

- execution progress only
- artifact-bearing outputs

Artifact-bearing task results are first-class in BrewDB and must not be treated as plain compute completions.

### Output shape by task role

- append materialization tasks return staged append artifact references and summaries
- rewrite materialization tasks return staged mutation artifact references plus affected-scope summaries
- selection tasks return candidate-set fragments and summaries

Task contracts remain execution-scoped and must not include transaction, lease, or final commit truth.

## 10. Execution Outputs

Phase 1 execution may produce:

- result streams
- shuffle/exchange outputs
- staged data artifacts
- staged mutation artifacts
- candidate-set artifacts
- task/stage-level manifests

Execution outputs are not final commits. They are inputs to later control-plane decisions.

## 11. Materialization Semantics

Materialization is a first-class execution outcome.

It is used to:

- persist append-ready artifacts
- persist rewrite-ready artifacts
- persist candidate sets for later stages

Phase 1 materialization does not imply resumable execution semantics. The system may persist outputs without promising restart from those outputs after coordinator loss.

## 12. Design Rules

1. Stage boundaries are driven by both distributed execution needs and lifecycle materialization needs.
2. Materialization is part of the main execution framework, not a side path.
3. Selection/candidate boundaries are first-class execution outcomes for maintenance-style jobs.
4. Workers may return artifact-bearing outputs, not only execution completion.
5. Worker runtimes are execution-rich but control-plane-thin.
6. Task requests carry execution fragments plus boundary-aware output contracts.
7. Task results report execution status plus boundary outputs.
8. Execution outputs remain non-authoritative until the control plane validates and publishes them.
