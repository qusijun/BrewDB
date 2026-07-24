# BrewDB Coordinator Phase 1

This document defines the Phase 1 coordinator model for BrewDB. It focuses on control-plane behavior, runtime state, commit flow, and recovery. It does not define Rust interfaces or long-task resumability.

## 1. Scope

Phase 1 coordinator supports:

- peer request admission across all cluster nodes
- single-owner execution per job
- distributed query / mutation / maintenance / DDL orchestration
- centralized table-visible commit
- commit truth recovery and artifact cleanup

Phase 1 explicitly does not support:

- in-flight job takeover
- long-task resume after coordinator loss
- BSP-style execution recovery
- cross-format atomic transactions
- global TSO

## 2. Coordinator Model

All cluster nodes may act as coordinators for admission, planning, and scheduling.

For each job:

- one coordinator becomes the owner for the lifetime of the job
- coordinator loss causes the in-flight job to fail
- the system recovers commit truth and cleans up artifacts, but does not resume execution

This is a peer-admission, single-owner, no-resume model.

## 3. Internal Modules

### `Gateway`

- SQL / HTTP entry
- session and auth context
- request/response handling

### `CatalogFacade`

- Lakekeeper access
- namespace and table resolution
- warehouse and credential lookup
- format routing

### `Planner`

- query / mutation / maintenance / DDL planning
- capability checks
- stage/task graph construction
- resource-lane requirements

### `Dispatcher`

- stage/task scheduling
- worker assignment
- task retry
- stage progress aggregation

### `JobManager`

- job creation and ownership
- job lifecycle state
- resource wait handling
- final success/failure/cancel transitions

### `TxnManager`

- transaction context creation
- transaction state truth
- idempotency keys
- unknown-outcome handling

### `CommitManager`

- artifact bundle aggregation
- validation and publish orchestration
- commit-attempt history
- adapter commit execution

## 4. State Objects

### `Job`

BrewDB-native lifecycle object.

States:

- `pending`
- `planning`
- `running`
- `waiting_resource`
- `committing`
- `aborting`
- `succeeded`
- `failed`
- `canceled`

### `Stage`

Execution-boundary object.

States:

- `pending`
- `schedulable`
- `running`
- `succeeded`
- `failed`
- `canceled`

### `TaskAttempt`

Worker execution attempt object.

States:

- `pending`
- `assigned`
- `running`
- `succeeded`
- `failed`
- `canceled`

Commit is not a stage/task concern. Commit is modeled at job/transaction level.

## 5. Transaction and Commit Model

Phase 1 relations:

- `Job -> Txn`: `0..1`
- `Txn -> CommitAttempt`: `1..N`

Not every job has a transaction. Query jobs typically do not.

### `TxnState`

- `open`
- `validating`
- `committing`
- `committed`
- `aborting`
- `aborted`
- `unknown_outcome`

### `CommitAttemptState`

- `created`
- `validating`
- `publishing`
- `succeeded`
- `failed`
- `unknown_outcome`

Rules:

- a txn-bearing job succeeds only after `TxnState=committed`
- `CommitAttempt.failed` does not imply `TxnState=aborted`
- `CommitAttempt.unknown_outcome` implies `TxnState=unknown_outcome` until reconciliation completes

## 6. Ownership and Leases

Phase 1 uses a minimal coordination model.

### `JobOwnerRecord`

- records which coordinator owns a job
- prevents duplicate lifecycle advancement
- is not a takeover lease in Phase 1

### `TableResourceLease`

Each table has three logical lanes:

- `ddl`
- `mutation`
- `maintenance`

Phase 1 uses conservative serialization in critical sections:

- `ddl` conflicts with `ddl`, `mutation`, and `maintenance`
- `mutation` conflicts with `mutation`
- `maintenance` conflicts with `maintenance`
- `mutation` and `maintenance` are separate lanes, but still conflict in Phase 1 critical sections

### `ClusterHousekeepingLease`

Used for cluster-wide background duties such as:

- failed-job scanning
- reconciliation scanning
- orphan artifact scanning

## 7. Lease Timing

Table leases are not acquired at job start.

They are acquired when the job enters a critical section and are held until commit finalization or abort completes.

Examples:

- `INSERT SELECT`: acquire `mutation` lane after distributed execution finishes
- `COMPACT`: acquire `maintenance` lane after candidate-set selection
- `ALTER TABLE`: acquire `ddl` lane before metadata mutation

Lane acquisition policy is:

- try acquire
- bounded wait
- timeout fail
- no built-in persistent queue

## 8. Runtime Metadata Families

### Lifecycle

- `JobRecord`
- `StageRecord`
- `TaskAttemptRecord`

### Commit

- `TxnRecord`
- `CommitAttemptRecord`
- `ReconciliationRecord`

### Coordination

- `JobOwnerRecord`
- `ResourceLeaseRecord`
- `ClusterLeaseRecord`

### Artifacts

- `ArtifactManifest`
- `ArtifactBundleRecord`

Primary-writer rule:

- `JobManager` writes `JobRecord`
- `Dispatcher` writes `StageRecord`
- worker/dispatcher paths write `TaskAttemptRecord`
- `TxnManager` writes `TxnRecord`
- `CommitManager` writes `CommitAttemptRecord`

## 9. Consistency Boundaries

Strong atomic updates are required for:

- job creation with owner registration
- job-to-txn association
- attempt creation with txn state transition
- `publishing` attempt with `TxnState=committing`
- successful attempt with `TxnState=committed`
- unknown-outcome attempt with `TxnState=unknown_outcome`
- reconciliation result with final txn resolution

Eventually consistent or async-updated data is acceptable for:

- task metrics
- artifact statistics
- progress summaries
- dashboard-oriented derived fields

## 10. Representative Flows

### `INSERT SELECT`

1. plan query and write path
2. run distributed stages and produce staged outputs
3. acquire `mutation` lane
4. create txn
5. validate and publish through commit attempts
6. finalize as success, failure, or unknown outcome

### `COMPACT`

1. select compact candidates
2. acquire `maintenance` lane
3. run distributed rewrite
4. create txn
5. validate replacement set and publish

### `ALTER TABLE`

1. plan DDL
2. acquire `ddl` lane
3. create txn
4. publish metadata change

## 11. Recovery Rules

Coordinator-loss recovery is fail-and-reconcile, not fail-and-resume.

When a coordinator dies:

- jobs in `pending`, `planning`, `running`, or `waiting_resource` become `failed`
- `TxnState=open|validating` is resolved to `aborted`
- `TxnState=committing` is promoted to `unknown_outcome`
- `TxnState=committed` may allow a job to be finalized as `succeeded`
- `TxnState=unknown_outcome` requires reconciliation before artifact cleanup

Recovery responsibilities:

- mark jobs failed when ownership is lost
- resolve transaction truth
- create reconciliation work for unknown outcomes
- release dangling leases after state convergence
- clean up staged artifacts according to final txn outcome

## 12. Phase 2+ Direction

Later phases may add:

- real job-takeover leases
- stage-boundary restart
- durable intermediate state
- BSP-style long-task recovery
- less conservative lane concurrency

Phase 1 intentionally stops short of those features.
