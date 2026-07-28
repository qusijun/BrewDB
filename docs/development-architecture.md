# BrewDB Development Architecture

This document defines the Phase 1 development architecture for BrewDB. It translates the logical architecture into a crate/module structure for implementation. It does not define storage schemas, RPC wire formats, or Rust trait signatures.

## 1. Scope

Phase 1 implementation is organized as a monorepo with a small number of capability-oriented crates.

The crate layout is intentionally not based on deployment roles such as coordinator or worker. Binary packaging should also avoid turning those roles into the primary repository boundary.

## 2. Crate Layout

Phase 1 uses six primary crates.

### `brewdb-core`

Shared domain language:

- ids
- states
- common errors
- table envelope basics
- capability model
- job / stage / task / txn / artifact core concepts

### `brewdb-catalog`

Catalog control-plane access kernel:

- Lakekeeper-facing facade
- cache
- normalization
- route and handle resolution
- warehouse/storage profile resolution

### `brewdb-sql`

Language frontend:

- parsing
- binding
- statement analysis
- SQL rewrites
- intent generation
- SQL-surface capability gating

### `brewdb-execution`

Distributed execution kernel:

- stage/task model
- boundary kinds
- task request/result contract
- worker runtime shared logic
- materialization contracts
- execution-side data cache

### `brewdb-storage`

Storage semantics kernel:

- table-level adapters
- scan/append/rewrite/maintenance/commit semantics
- format truth interpretation
- concrete format implementations such as Paimon and Iceberg

### `brewdb-runtime`

Lifecycle and control kernel:

- job orchestration
- transaction orchestration
- commit orchestration
- lease handling
- recovery
- mutation orchestration
- maintenance orchestration

## 2.1 Crate Responsibility Matrix

To keep the six crates from drifting into each other, each crate should be judged by four questions:

- what truth does it own
- what inputs it consumes
- what outputs it produces
- what it must not own

### `brewdb-core`

Owns:

- shared domain vocabulary
- identifiers and state enums
- cross-crate error and context types

Consumes:

- no BrewDB crate

Produces:

- stable shared types reused by all upper layers

Must not own:

- orchestration
- transport
- format-specific semantics
- control-plane IO

### `brewdb-catalog`

Owns:

- control-plane access facade
- normalized table envelope
- catalog cache and route resolution

Consumes:

- `brewdb-core`
- Lakekeeper-facing clients and control-plane integrations

Produces:

- normalized namespace/table metadata
- warehouse and credential route information
- format handle lookup results

Must not own:

- job lifecycle
- transaction truth
- final format commit semantics
- execution planning

### `brewdb-sql`

Owns:

- SQL parsing and binding
- statement analysis
- intent generation
- SQL-surface capability checks

Consumes:

- `brewdb-core`
- `brewdb-catalog`
- `brewdb-runtime` planning entry points

Produces:

- query intent
- insert intent
- mutation intent
- maintenance intent
- DDL intent

Must not own:

- physical execution plans
- adapter-native semantics
- runtime-state persistence

### `brewdb-execution`

Owns:

- stage graph and task model
- execution-side physical planning
- task request/result contract
- materialization and boundary output contracts
- executor runtime behavior
- execution-graph boundary semantics

Consumes:

- `brewdb-core`
- selective execution-facing requirements from upper layers

Produces:

- stage graphs
- task payloads
- task results
- staged artifact references
- boundary summaries

Must not own:

- job ownership
- table leases
- transaction state
- commit journaling
- catalog mutation

### `brewdb-storage`

Owns:

- table-level adapter boundary
- scan/append/rewrite/maintenance/commit semantics
- format truth interpretation
- reconciliation truth lookup

Consumes:

- `brewdb-core`
- `brewdb-catalog`
- selective `brewdb-execution` contracts for artifact and scan shape requirements

Produces:

- capability views
- scan and materialization requirements
- commit validation and publish behavior
- reconciliation behavior

Must not own:

- SQL parsing
- runtime orchestration
- distributed scheduling
- Lakekeeper facade ownership

### `brewdb-runtime`

Owns:

- job lifecycle
- transaction lifecycle
- transaction management locking
- commit orchestration shell
- lease and recovery framework
- mutation and maintenance orchestration
- scheduler policy and dispatch coordination

Consumes:

- `brewdb-core`
- `brewdb-catalog`
- `brewdb-execution`
- `brewdb-storage`

Produces:

- orchestration plans
- dispatch decisions
- runtime-state transitions
- commit attempts
- recovery actions

Must not own:

- format-native truth
- task execution internals
- raw control-plane client details
- SQL syntax concerns

## 2.2 Crate Collaboration Paths

The main allowed call paths should stay narrow:

1. SQL path:
   `brewdb-sql -> brewdb-catalog -> brewdb-runtime -> brewdb-execution / brewdb-storage`
2. Commit path:
   `brewdb-runtime -> brewdb-catalog -> brewdb-storage`
3. Execution path:
   `brewdb-runtime -> brewdb-execution`
4. Recovery path:
   `brewdb-runtime -> brewdb-catalog + brewdb-storage`

The main disallowed shortcuts are:

- `brewdb-sql -> brewdb-storage`
- `brewdb-execution -> brewdb-runtime`
- `brewdb-execution -> brewdb-catalog`
- `brewdb-storage -> brewdb-runtime`
- upper layers depending on raw Lakekeeper response types

## 2.3 Crate Public Surface

Each crate should expose a small, intentional top-level surface. Public API design should optimize for stable collaboration boundaries, not convenience re-export sprawl.

### `brewdb-core`

Recommended public surface:

- `ids`
- `state`
- `catalog`
- `execution`
- `txn`
- `artifacts`
- `errors`
- `common`

Recommended internal-only details:

- state transition helpers that are purely crate-local
- serialization glue that exists only for one caller
- test-only builders and fixtures

Public API rule:

- `brewdb-core` may expose types freely, but should avoid exposing behavior that implies orchestration ownership

### `brewdb-catalog`

Recommended public surface:

- `facade`
- `model`
- `route`
- `warehouse`
- `errors`

Recommended internal-only details:

- `client`
- `cache`
- `normalize`
- vendor- or Lakekeeper-specific transport modules

Public API rule:

- callers should depend on catalog-facing models and facade entry points, never on raw control-plane client types

### `brewdb-sql`

Recommended public surface:

- `ast`
- `bind`
- `analyze`
- `rewrite`
- `intent`
- `capabilities`
- `errors`

Recommended internal-only details:

- parser implementation glue
- tokenization details
- frontend-only normalization helpers

Public API rule:

- the most important stable outputs are intent objects, not parser internals

### `brewdb-execution`

Recommended public surface:

- `plan`
- `task`
- `boundaries`
- `artifacts`
- `errors`

Conditionally public surface:

- `runtime` only for clearly shared runtime contracts

Recommended internal-only details:

- executor implementation internals
- local scheduling details
- spill/shuffle implementation specifics
- worker-local service wiring
- metrics backend bindings

Public API rule:

- upper layers may see execution contracts, but should not bind to executor implementation structure

### `brewdb-storage`

Recommended public surface:

- `adapter`
- `model`
- `route`
- `errors`

Conditionally public surface:

- `scan`
- `append`
- `rewrite`
- `maintenance`
- `commit`

These operation modules may remain public if they define stable adapter-facing contracts. If they become implementation-heavy, they should collapse behind `adapter`.

Recommended internal-only details:

- format-specific helper modules
- metadata parsing internals
- format-native reconciliation subroutines
- per-format optimization heuristics

Public API rule:

- callers should depend on table-adapter contracts, not on per-format implementation modules

### `brewdb-runtime`

Recommended public surface:

- `jobs`
- `txns`
- `locks`
- `commit`
- `recovery`
- `leases`
- `planning`
- `errors`

Conditionally public surface:

- `mutation`
- `maintenance`
- `runtime`

Recommended internal-only details:

- orchestration step executors
- persistence adapters
- retry loops
- background housekeeping internals

Public API rule:

- other crates should depend on orchestration entry points and shared lifecycle models, not on step-by-step coordinator internals

## 2.4 Re-export Policy

To prevent public API drift, the workspace should follow a strict re-export policy.

Allowed:

- re-exporting small, stable domain types that improve ergonomics
- re-exporting crate-local facade entry points
- re-exporting intentionally stable contract modules

Avoid:

- wildcard re-exports across major submodules
- re-exporting implementation helpers only because another crate currently uses them
- exposing vendor-specific or transport-specific types at the crate root

Default rule:

- if a module is not part of a crate's architecture boundary, do not make it part of the public prelude

## 2.5 Main Type Ownership

The architecture should also be explicit about where the main system objects live. If a type is shared across crate boundaries, its home crate should be chosen by ownership truth, not convenience.

### `brewdb-core`

Should own the canonical type definitions for:

- `JobId`
- `StageId`
- `TaskId`
- `TaskAttemptId`
- `TxnId`
- `CommitAttemptId`
- `ArtifactId`
- `NamespaceId`
- `TableId`
- `WarehouseId`
- `JobState`
- `StageState`
- `TaskAttemptState`
- `TxnState`
- `CommitAttemptState`
- `ResourceLane`
- `TxnLockRecord`
- request/session context primitives

Rule:

- if a type mainly expresses shared domain identity or shared lifecycle state, it belongs in `brewdb-core`

### `brewdb-catalog`

Should own:

- `NamespaceEnvelope`
- `TableEnvelope`
- `WarehouseProfile`
- `CatalogRoute`
- `FormatHandle`

Rule:

- if a type represents normalized control-plane truth or control-plane routing, it belongs in `brewdb-catalog`

### `brewdb-sql`

Should own:

- AST types
- bound statement types
- SQL intent types
- frontend capability diagnostics

Rule:

- if a type is meaningful only before orchestration handoff, it belongs in `brewdb-sql`

### `brewdb-execution`

Should own:

- `StageGraph`
- `StagePlan`
- `TaskRequest`
- `TaskResult`
- `BoundaryKind`
- `MaterializationContract`
- `SelectionResult`
- execution-facing artifact summary types

Rule:

- if a type describes execution slicing, task dispatch, or boundary outputs, it belongs in `brewdb-execution`

### `brewdb-storage`

Should own:

- table adapter interfaces
- scan requirement types
- append/rewrite realization types
- commit validation request/result types
- reconciliation request/result types
- format capability views

Rule:

- if a type expresses format-aware semantics or adapter contracts, it belongs in `brewdb-storage`

### `brewdb-runtime`

Should own:

- `JobRecord`
- `TxnRecord`
- `CommitAttemptRecord`
- `ResourceLeaseRecord`
- runtime orchestration plan types
- recovery work item types
- dispatch decision types

Rule:

- if a type represents runtime truth, orchestration truth, or recovery truth, it belongs in `brewdb-runtime`

## 2.6 Runtime Metadata Boundary

Runtime metadata should be modeled as a first-class framework boundary, not just a persistence detail.

The runtime store should be owned by `brewdb-runtime`, but its records should connect cleanly to the rest of the workspace.

### Runtime-store families

Lifecycle records:

- `JobRecord`
- `StageRecord`
- `TaskAttemptRecord`

Transaction records:

- `TxnRecord`
- `CommitAttemptRecord`
- `TxnLockRecord`
- `ReconciliationRecord`

Coordination records:

- `JobOwnerRecord`
- `ResourceLeaseRecord`
- `ClusterLeaseRecord`

Artifact records:

- `ArtifactManifest`
- `ArtifactBundleRecord`

Design rules:

- `brewdb-runtime` owns write authority for runtime truth
- `brewdb-execution` may emit task facts, but should not own authoritative runtime transitions
- `brewdb-storage` may return validation/publish truth, but should not persist runtime orchestration truth directly
- `brewdb-catalog` remains outside runtime metadata ownership even when backed by the same PostgreSQL instance

## 2.7 Main Cross-Crate Framework Flows

The architecture should keep a few primary end-to-end flows explicit so crate boundaries can be judged against real movement, not only static responsibility lists.

### Query flow

1. `brewdb-sql` parses, binds, and emits query intent
2. `brewdb-catalog` resolves table envelopes and warehouse routes
3. `brewdb-runtime` shapes orchestration and dispatch requirements
4. `brewdb-execution` builds stage graphs and runs tasks
5. results return through runtime-owned job truth, without creating txn/commit state

### Append flow

1. `brewdb-sql` emits insert intent
2. `brewdb-catalog` resolves target table envelope
3. `brewdb-storage` returns append requirements
4. `brewdb-runtime` creates job and txn shell
5. `brewdb-execution` materializes staged append artifacts
6. `brewdb-runtime` acquires resource lane and txn lock
7. `brewdb-storage` validates and publishes the final append
8. `brewdb-runtime` resolves commit truth and cleanup eligibility

### Rewrite mutation flow

1. `brewdb-sql` emits rewrite mutation intent
2. `brewdb-catalog` resolves target envelope
3. `brewdb-storage` returns rewrite realization requirements
4. `brewdb-runtime` shapes job and critical-section timing
5. `brewdb-execution` scans, matches, and materializes staged mutation artifacts
6. `brewdb-runtime` enters mutation lane and txn finalization
7. `brewdb-storage` validates and publishes format-native mutation results
8. `brewdb-runtime` resolves txn and artifact lifecycle truth

### Recovery flow

1. `brewdb-runtime` detects ownership loss or unknown outcome
2. `brewdb-runtime` loads runtime truth from the runtime store
3. `brewdb-catalog` reloads current table route and control-plane bindings
4. `brewdb-storage` resolves external format truth
5. `brewdb-runtime` finalizes txn resolution, lease cleanup, and artifact cleanup eligibility

Framework rule:

- every major user-visible operation should map onto one of these cross-crate flow shapes or a close variation of them

## 2.8 Runtime State Machine Matrix

The runtime framework should also freeze the coupling between its main state objects. Phase 1 should not let each subsystem invent its own interpretation of job, transaction, commit, and lock progression.

### Main runtime objects

- `JobRecord`
- `TxnRecord`
- `CommitAttemptRecord`
- `TxnLockRecord`

### State-object roles

`JobRecord`:

- user-visible lifecycle truth for one admitted operation

`TxnRecord`:

- transaction lifecycle truth for one commit-bearing operation

`CommitAttemptRecord`:

- one concrete validate/publish attempt under a txn

`TxnLockRecord`:

- exclusive lifecycle-driving authority for one txn at a time

## 2.8.1 Job and Transaction Coupling

Allowed high-level coupling:

- a query job may have no txn
- an append/rewrite/maintenance/DDL finalization job may create one txn
- a txn-bearing job may not become `succeeded` before its txn becomes `committed`
- a txn-bearing job may not become terminal-success while its txn is `open`, `validating`, `committing`, or `unknown_outcome`

Expected mapping:

- `JobState=pending|planning|running|waiting_resource` -> txn absent or pre-finalization
- `JobState=committing` -> txn should exist
- `JobState=succeeded` -> txn absent or `TxnState=committed`
- `JobState=failed|canceled` -> txn absent, `aborted`, or unresolved only during recovery convergence

## 2.8.2 Transaction State Matrix

Phase 1 transaction states:

- `open`
- `validating`
- `committing`
- `committed`
- `aborting`
- `aborted`
- `unknown_outcome`

Allowed transitions:

- `open -> validating`
- `open -> aborting`
- `validating -> committing`
- `validating -> aborting`
- `committing -> committed`
- `committing -> unknown_outcome`
- `committing -> aborting` only if publish authority was not yet externally exercised
- `aborting -> aborted`
- `unknown_outcome -> committed`
- `unknown_outcome -> aborted`

Disallowed shortcuts:

- `open -> committed`
- `open -> unknown_outcome`
- `validating -> committed`
- `committed ->` any non-terminal state
- `aborted ->` any non-terminal state

Interpretation rule:

- `unknown_outcome` means BrewDB lost certainty about external publish truth, not that the txn is semantically failed

## 2.8.3 Commit Attempt State Matrix

Phase 1 commit-attempt states:

- `created`
- `validating`
- `publishing`
- `succeeded`
- `failed`
- `unknown_outcome`

Allowed transitions:

- `created -> validating`
- `validating -> publishing`
- `validating -> failed`
- `publishing -> succeeded`
- `publishing -> failed`
- `publishing -> unknown_outcome`

Disallowed shortcuts:

- `created -> publishing`
- `created -> succeeded`
- `failed -> publishing`
- `succeeded ->` any non-terminal state
- `unknown_outcome -> publishing`

Coupling rules:

- at most one active publish-bearing attempt may exist per txn at a time
- a new retry attempt requires ownership of `TxnLockRecord`
- `CommitAttemptRecord=succeeded` should imply `TxnState=committed` in the same convergence boundary
- `CommitAttemptRecord=unknown_outcome` should imply `TxnState=unknown_outcome`

## 2.8.4 Transaction Lock State Matrix

`TxnLockRecord` should be modeled as a lease-like authority object rather than a domain transaction state.

Useful lock states:

- `free`
- `held`
- `expired`

Allowed transitions:

- `free -> held`
- `held -> held` by heartbeat or fencing renewal
- `held -> expired`
- `expired -> held` by fenced reacquire
- `held -> free` by explicit release

Rules:

- only one `held` authority may be valid for the latest fencing epoch
- reconciliation reacquire must use a newer fencing epoch than the lost owner
- external publish callbacks or completions from stale epochs must be ignored

Implementation rule:

- `TxnLockRecord` may be persisted as a record with timestamps and epoch rather than an enum-backed state machine, but its semantics should still follow the transitions above

## 2.8.5 Cross-Object Invariants

The most important Phase 1 invariants are:

1. one job owner at a time may advance a non-recovery job lifecycle
2. one txn lock holder at a time may advance a txn lifecycle
3. one publish-bearing commit attempt at a time may represent the active external publish owner
4. `JobState=succeeded` implies no unresolved txn truth
5. `TxnState=unknown_outcome` blocks aggressive artifact cleanup
6. `TxnState=committed` allows success finalization only after commit-attempt convergence
7. `TxnState=aborted` allows failure or cancel finalization plus cleanup eligibility evaluation

## 2.8.6 Runtime State Ownership by Crate

Ownership should remain explicit:

- `brewdb-core` owns the shared enums and identity types
- `brewdb-runtime` owns the runtime records and allowed transition enforcement
- `brewdb-storage` may influence transaction outcome through validation/publish truth, but does not own the runtime transition graph
- `brewdb-execution` may complete tasks and emit artifacts, but does not advance txn or commit-attempt state directly

Framework rule:

- if a state transition changes job truth, txn truth, commit truth, or recovery truth, it belongs under `brewdb-runtime`

## 3. Dependency Direction

Recommended dependency direction:

- `brewdb-core`
- `brewdb-catalog -> brewdb-core`
- `brewdb-execution -> brewdb-core`
- `brewdb-storage -> brewdb-core + brewdb-catalog + selective brewdb-execution contracts`
- `brewdb-runtime -> brewdb-core + brewdb-catalog + brewdb-execution + brewdb-storage`
- `brewdb-sql -> brewdb-core + brewdb-catalog + brewdb-runtime`

Key rules:

- `brewdb-core` depends on no other BrewDB crate
- `brewdb-sql` does not directly depend on storage semantics
- `brewdb-storage` does not depend on kernel orchestration
- `brewdb-execution` does not depend on kernel orchestration

## 4. Planner and Optimizer Placement

Planner responsibilities are split across three layers.

### `brewdb-sql`

Holds:

- SQL-facing planning
- binder logic
- statement classification
- frontend semantic rewrites
- lightweight frontend optimization

### `brewdb-runtime::planning`

Holds:

- intent-to-execution orchestration planning
- lane timing and handoff planning
- mutation/maintenance job shaping
- bundle and commit handoff planning

The coordinator-side optimizer selection baseline for this layer is defined in `docs/coordinator-cbo-optimizer-selection.md`.

### Scheduler baseline

Phase 1 scheduler should be treated as:

- MPP-first in runtime behavior
- graph-wide in admission
- dependency-driven in dispatch
- boundary-aware in release conditions
- policy-extensible toward future BSP or superstep execution

Recommended ownership split:

- `brewdb-execution`
  - `StageGraph`
  - task dependency descriptors
  - boundary semantics such as `pipelined | materialized | barriered`
- `brewdb-runtime`
  - worker assignment
  - dispatch throttling
  - runnable-set release
  - retry policy
  - future scheduling policy selection

### `brewdb-execution`

Holds:

- physical execution planning
- stage shaping
- execution-facing optimization
- boundary placement logic

## 5. Module Layout by Crate

### `brewdb-core`

Recommended modules:

- `ids`
- `state`
- `catalog`
- `execution`
- `txn`
- `artifacts`
- `errors`
- `common`

`brewdb-core` should remain a stable shared language layer and avoid orchestration logic.

### `brewdb-catalog`

Recommended modules:

- `facade`
- `client`
- `cache`
- `normalize`
- `route`
- `warehouse`
- `model`

### `brewdb-sql`

Recommended modules:

- `parse`
- `ast`
- `bind`
- `analyze`
- `rewrite`
- `intent`
- `capabilities`
- `errors`

### `brewdb-execution`

Recommended modules:

- `plan`
- `task`
- `runtime`
- `boundaries`
- `artifacts`
- `worker`
- `cache`
- `metrics`

### `brewdb-storage`

Recommended modules:

- `adapter`
- `scan`
- `append`
- `rewrite`
- `maintenance`
- `commit`
- `route`
- `model`
- `formats`

### `brewdb-runtime`

Recommended modules:

- `jobs`
- `txns`
- `locks`
- `commit`
- `recovery`
- `leases`
- `mutation`
- `maintenance`
- `planning`
- `runtime`

## 6. Binary Assembly

Crates are capability-oriented, and binary assembly should follow product or interface boundaries rather than internal runtime roles.

Phase 1 may expose binaries such as:

- `brewdbd`
- `brewdb`

## 7. Workspace Bootstrap

Phase 1 should start as one Rust workspace with capability crates and thin binaries.

Recommended top-level layout:

- `crates/brewdb-core`
- `crates/brewdb-catalog`
- `crates/brewdb-sql`
- `crates/brewdb-execution`
- `crates/brewdb-storage`
- `crates/brewdb-runtime`
- `bin/brewdbd`
- `bin/brewdb`

Non-crate top-level directories may include:

- `docs`
- `proto` or `api` for RPC contracts after Phase 1 interfaces stabilize
- `tests` for cross-crate integration scenarios

The workspace should begin with crate boundaries first, even if the first running binary keeps most paths in-process.

## 7.1 Repository Code Structure

Phase 1 should freeze a repository layout early so later code motion does not quietly redefine architecture.

Recommended top-level tree:

```text
BrewDB/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── clippy.toml
├── rustfmt.toml
├── docs/
├── crates/
│   ├── brewdb-core/
│   ├── brewdb-catalog/
│   ├── brewdb-sql/
│   ├── brewdb-execution/
│   ├── brewdb-storage/
│   └── brewdb-runtime/
├── bin/
│   ├── brewdbd/
│   └── brewdb/
├── tests/
│   ├── integration/
│   ├── fixtures/
│   └── harness/
└── scripts/
```

Top-level rules:

- all reusable product code lives under `crates/`
- all product-facing binaries live under `bin/`
- cross-crate integration tests live under `tests/`
- repo automation and local developer utilities live under `scripts/`
- architecture and design material stays under `docs/`

## 7.2 Standard Crate Skeleton

Every library crate should begin with a predictable internal shape.

Recommended baseline:

```text
crates/<crate-name>/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── errors.rs
│   ├── model/        # optional when a crate has a heavy model surface
│   ├── service/      # optional when a crate has orchestration/service entry points
│   └── ...
└── tests/            # only when crate-local integration tests are useful
```

Rules:

- `lib.rs` should be small and mostly define the public module map
- implementation-heavy helpers should stay below one extra directory level instead of crowding `lib.rs`
- crate-local `tests/` is for focused contract tests; cross-crate scenarios still belong in workspace `tests/`

## 7.3 Recommended Source Layout By Crate

The repo should not stop at crate names. Each crate should start with a predictable source tree.

### `crates/brewdb-core`

```text
src/
├── lib.rs
├── ids.rs
├── state.rs
├── catalog.rs
├── execution.rs
├── txn.rs
├── artifacts.rs
├── errors.rs
└── common.rs
```

Rule:

- keep `brewdb-core` flat unless one module becomes too large; it is the shared language crate, not a deep service tree

### `crates/brewdb-catalog`

```text
src/
├── lib.rs
├── facade.rs
├── model.rs
├── route.rs
├── warehouse.rs
├── errors.rs
├── client/
├── cache/
└── normalize/
```

Rule:

- stable caller-facing APIs stay near the top; transport and cache machinery move into subdirectories

### `crates/brewdb-sql`

```text
src/
├── lib.rs
├── ast.rs
├── bind.rs
├── analyze.rs
├── rewrite.rs
├── intent.rs
├── capabilities.rs
├── errors.rs
└── parse/
```

Rule:

- parser internals should stay behind `parse/`; upper layers should mostly see AST, bound forms, and intent outputs

### `crates/brewdb-execution`

```text
src/
├── lib.rs
├── plan.rs
├── task.rs
├── boundaries.rs
├── artifacts.rs
├── errors.rs
├── runtime/
├── worker/
├── cache/
└── metrics/
```

Rule:

- contract modules stay at top level; executor/runtime implementation details go into subdirectories

### `crates/brewdb-storage`

```text
src/
├── lib.rs
├── adapter.rs
├── model.rs
├── route.rs
├── errors.rs
├── scan/
├── append/
├── rewrite/
├── maintenance/
├── commit/
└── formats/
    ├── paimon/
    └── iceberg/
```

Rule:

- format-neutral contracts stay at top level; format-specific implementations stay under `formats/`

### `crates/brewdb-runtime`

```text
src/
├── lib.rs
├── errors.rs
├── admission/
├── jobs/
├── txns/
├── locks/
├── commit/
├── recovery/
├── leases/
├── mutation/
├── maintenance/
├── planning/
└── runtime/
```

Rule:

- runtime state and orchestration subsystems should live in dedicated directories, because this crate will grow fastest

## 7.4 Binary Package Layout

Binary packages should stay thin and mostly assemble shared crates.

### `bin/brewdbd`

Recommended structure:

```text
bin/brewdbd/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── config.rs
    ├── bootstrap.rs
    ├── server.rs
    └── wiring/
```

### `bin/brewdb`

Recommended structure:

```text
bin/brewdb/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── cli.rs
    ├── config.rs
    └── commands/
```

Binary rules:

- binaries should wire config, dependency assembly, and interface surfaces
- binaries should not become the home of shared domain logic
- if logic becomes reusable across binaries, move it back to `crates/`

## 7.5 Test Layout

Testing should also follow a fixed repository shape.

Recommended layout:

```text
tests/
├── integration/
│   ├── append_flow.rs
│   ├── commit_recovery.rs
│   └── mutation_flow.rs
├── fixtures/
│   ├── catalog/
│   ├── runtime/
│   └── storage/
└── harness/
    ├── mod.rs
    ├── cluster.rs
    ├── catalog.rs
    └── object_store.rs
```

Rules:

- reusable fake services and test harness code live under `tests/harness`
- static inputs and golden data live under `tests/fixtures`
- scenario-oriented end-to-end tests live under `tests/integration`

## 7.6 Code Structure Freeze Rules

Once implementation starts, the following structure rules should be treated as frozen unless an architecture review reopens them:

1. shared logic does not move into `bin/`
2. format-specific code does not leak out of `brewdb-storage/formats/`
3. runtime truth and orchestration code does not leak out of `brewdb-runtime/`
4. cross-crate integration scenarios do not get buried inside one library crate
5. top-level repository directories are added only for a new architecture concern, not short-term convenience

## 7.7 Workspace Cargo Strategy

The workspace should also freeze how Cargo metadata is organized, so dependency sprawl does not redefine architecture by accident.

Recommended workspace root files:

- `Cargo.toml`
- `Cargo.lock`
- `rust-toolchain.toml`
- `clippy.toml`
- `rustfmt.toml`

Recommended root `Cargo.toml` responsibilities:

- declare all workspace members
- define shared edition, version, license, and repository metadata
- define `[workspace.dependencies]` for deliberately shared dependencies
- define shared lint policy once the codebase begins to compile

The root manifest should not:

- become the home of product code
- hide crate boundaries with path tricks
- turn every third-party library into a mandatory global dependency

## 7.8 Workspace Dependency Policy

Dependencies should be shared only when that reduces real inconsistency.

### Put in `[workspace.dependencies]`

Good candidates:

- foundational libraries used across many crates
- async/runtime primitives if Phase 1 standardizes on one stack
- serialization crates used in shared contracts
- tracing/logging crates
- error-handling crates
- testing crates used consistently across workspace tests

Examples of likely shared categories:

- `anyhow` or equivalent application-error layer
- `thiserror`
- `serde`
- `tracing`
- `tokio` if runtime standardization is explicit

### Keep local to a crate

Good candidates:

- parser-specific libraries used only by `brewdb-sql`
- format-specific libraries used only by `brewdb-storage`
- transport-specific client libraries used only by binaries or catalog internals
- experimental dependencies not yet proven to be cross-workspace standards

Rule:

- a dependency belongs in `[workspace.dependencies]` only after it is clearly part of the shared engineering baseline, not merely because two crates happen to use it today

## 7.9 Dependency Direction Enforcement

The workspace should encode architectural direction in Cargo layout as much as possible.

Rules:

- `brewdb-core` must not depend on any BrewDB crate
- `brewdb-sql` must not depend directly on `brewdb-storage`
- `brewdb-execution` must not depend on `brewdb-runtime`
- `brewdb-storage` must not depend on `brewdb-runtime`
- `bin/*` may depend on library crates, but library crates must not depend on `bin/*`

Practical policy:

- when a dependency direction feels wrong, prefer moving a shared type downward into `brewdb-core` or narrowing a contract module rather than adding a reverse dependency

## 7.10 Feature Flag Strategy

Phase 1 should keep feature flags conservative.

Allowed feature-flag uses:

- optional format implementations inside `brewdb-storage`
- optional binaries or admin surfaces
- optional integration-test scaffolding

Avoid:

- feature flags that radically change core ownership boundaries
- circular "optional" dependency patterns that effectively bypass architecture rules
- transport/runtime flags that create multiple incompatible shared contract surfaces too early

Recommended shape:

- `brewdb-storage` may use format flags such as `paimon` and `iceberg`
- workspace integration tests may gate external-system fixtures separately
- core state and runtime truth types should not be feature-fragmented in Phase 1

## 7.11 Lint and Formatting Policy

Engineering consistency should be part of the code structure freeze.

Recommended policy:

- one workspace formatter configuration
- one workspace lint baseline
- crate-local lint suppressions only with a concrete reason

Rules:

- formatting should be repo-wide and non-controversial
- lint suppression should stay near the code it justifies
- architectural layering problems should not be papered over with lint ignores

## 7.12 Build and Test Command Baseline

The repo should standardize a small set of baseline commands from the beginning.

Recommended baseline:

- `cargo check --workspace`
- `cargo test --workspace`
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets`

Later additions may include:

- focused integration-test invocations
- format-specific test jobs
- local cluster smoke tests

Rule:

- if a new crate or binary meaningfully changes the baseline developer command set, that should be called out in architecture review rather than introduced silently

## 7.13 Module Visibility and Naming Conventions

The workspace should also freeze a few code-shape rules inside `src/` trees, so module growth stays legible.

### Visibility defaults

Recommended defaults:

- modules are private unless they are part of the crate's architecture surface
- helpers default to `pub(crate)` instead of `pub`
- structs and enums default to private fields unless external construction is part of the contract
- constructors and builders should be explicit rather than exposing writable state everywhere

Rules:

- if a symbol is only used inside one crate, do not make it public for convenience
- if a symbol is used by tests only, prefer test-local helpers or crate-local exposure instead of widening the public API
- widening visibility should be treated as an architecture change, not a casual refactor

### `lib.rs` policy

Each library crate should use `lib.rs` as a boundary map, not an implementation file.

Recommended shape:

- declare the crate's stable modules
- re-export a small number of intentionally stable entry points when useful
- avoid hiding deep implementation structure behind giant re-export blocks

Avoid:

- putting substantive orchestration or parsing logic directly in `lib.rs`
- turning `lib.rs` into a convenience dump of every submodule
- using wildcard public exports at the crate root

### Naming rules

Recommended naming bias:

- nouns for domain types such as `JobRecord`, `TableEnvelope`, `TaskResult`
- verbs or verb phrases for operations such as `validate_commit`, `resolve_route`, `build_stage_graph`
- plural module names for grouped domains such as `jobs`, `txns`, `leases`
- singular module names for narrow contract surfaces such as `adapter`, `route`, `model`

Avoid:

- vague module names such as `utils`, `misc`, `helpers`, `common2`
- role-ambiguous names that blur crate ownership
- abbreviations that are not already standard in the codebase

## 7.14 Internal Layering Inside Crates

Even within a crate, the code should still respect a small number of internal layers.

Recommended internal layering shape:

1. model or contract layer
2. service or orchestration layer
3. persistence, transport, or integration layer
4. local helper implementations

Examples:

- `brewdb-runtime`: records and state types -> orchestration services -> runtime-store adapters
- `brewdb-catalog`: normalized models -> facade -> cache/client integrations
- `brewdb-storage`: adapter contracts -> operation realizations -> format implementations

Rule:

- lower layers should not reach upward into orchestration logic just because they live in the same crate

## 7.15 Error Boundary Policy

Error shape is part of code structure because it defines how crates talk to each other under failure.

Recommended policy:

- each crate owns a small crate-local error surface
- cross-crate contracts should prefer structured errors over ad hoc strings
- `brewdb-core` may define shared error categories when they are genuinely common domain concepts

Rules:

- `brewdb-catalog` should not leak raw Lakekeeper client errors as its stable public error type
- `brewdb-storage` should not leak format-vendor error types as its stable public contract
- `brewdb-runtime` should translate storage/catalog/execution failures into runtime-relevant orchestration errors
- binaries may further wrap errors for CLI/server reporting, but should not become the canonical home of shared error semantics

## 7.16 Serialization Boundary Policy

Serialization concerns should be explicit so stable domain types do not accidentally become transport-first types.

Recommended policy:

- derive serialization on shared types only when there is a real cross-process or persistence need
- keep transport DTOs close to the transport boundary when they diverge from internal domain types
- do not let external API shape silently dictate internal type ownership

Rules:

- `brewdb-core` may carry serialization derives for true shared records and ids
- execution wire payload specifics should stay near `brewdb-execution` contracts or future transport modules
- binary-layer API payload wrappers should not leak backward into crate ownership decisions

## 8. Initial Interface Boundaries

Before detailed Rust traits are frozen, Phase 1 should preserve these interface seams.

### `brewdb-core`

Must define stable shared types for:

- ids for job, stage, task, txn, artifact, table, warehouse
- lifecycle enums
- error families
- basic capability descriptors
- lightweight request context and tenancy context

### `brewdb-catalog`

Should expose a facade-oriented API for:

- namespace and table resolution
- table envelope loading
- warehouse/storage profile lookup
- catalog handle routing

It should hide Lakekeeper-specific client details from upper layers.

### `brewdb-sql`

Should emit intent-level outputs rather than direct execution plans.

Useful intent families:

- query intent
- insert intent
- mutation intent
- maintenance intent
- DDL intent

### `brewdb-execution`

Should expose execution contracts, not coordinator ownership logic.

Useful contracts include:

- stage graph
- task request / task result
- stage boundary descriptors
- artifact result descriptors

### `brewdb-storage`

Should expose adapter-facing capabilities for:

- scan planning inputs
- append/rewrite materialization contracts
- validation and commit preparation
- maintenance strategy selection

### `brewdb-runtime`

Should own the main orchestration interfaces:

- job submission
- planning handoff
- dispatch coordination
- txn lifecycle handling
- commit orchestration
- recovery entry points

## 9. Bring-Up Order

Recommended implementation order:

1. `brewdb-core`
2. `brewdb-catalog`
3. `brewdb-storage` adapter kernel
4. `brewdb-execution` task contract and local runtime skeleton
5. `brewdb-runtime` job/txn/commit skeleton
6. `brewdb-sql`
7. binary assembly and end-to-end integration

Rationale:

- `brewdb-core` establishes language and state vocabulary
- `brewdb-catalog` and `brewdb-storage` define table and format boundaries early
- `brewdb-execution` can then shape task/result contracts around real artifact needs
- `brewdb-runtime` can orchestrate with fewer moving abstractions
- `brewdb-sql` should bind into already-stable lifecycle intents rather than forcing them prematurely

## 10. Phase 1 Walking Skeleton

The first end-to-end development milestone should be a minimal append path, not full query completeness.

Recommended walking skeleton:

1. coordinator accepts a single `INSERT SELECT`-like request
2. SQL layer produces an insert intent
3. catalog resolves table identity and warehouse profile
4. storage adapter returns append requirements for the target table format
5. execution layer runs a local or single-worker materialization stage
6. workers emit staged append artifact references
7. kernel records job, txn, and commit-attempt state in the runtime store
8. storage adapter validates and publishes the final commit
9. recovery path can inspect unknown commit outcome and reconcile it

This path exercises every major architecture plane with the smallest useful surface.

## 11. Testing Strategy by Layer

Phase 1 should test by boundary, not only by crate.

Recommended test emphasis:

- `brewdb-core`: pure unit tests for ids, state transitions, and invariants
- `brewdb-catalog`: facade tests with mocked Lakekeeper responses
- `brewdb-storage`: adapter contract tests per format
- `brewdb-execution`: task contract, boundary, and artifact result tests
- `brewdb-runtime`: job lifecycle, txn state, commit retry, and recovery tests
- workspace integration tests: append skeleton, failed commit, and coordinator-loss reconciliation

The first integration tests should focus on staged artifact correctness and commit truth recovery, because those are the main non-query risks in the architecture.

## 12. Early Non-Goals for Code Structure

Phase 1 should avoid:

- separate crates for every submodule too early
- format-specific orchestration logic leaking into `brewdb-runtime`
- direct SQL-layer dependence on concrete Lakekeeper or format clients
- worker binaries owning commit or catalog mutation logic
- premature RPC schema freeze before task/result contracts stabilize

The codebase should first optimize for clear ownership boundaries and a working lifecycle skeleton, then for service decomposition detail.

Those binaries should be assembly layers over shared crates. They should not define the crate structure.

## 13. Development Architecture Baseline

The following development decisions should be treated as the default Phase 1 baseline unless a later architecture review explicitly changes them.

### Repository shape

- one Rust workspace
- capability-oriented crates
- thin product-oriented binaries
- one repository for runtime, tooling, shared kernels, and integration tests

### Runtime topology for implementation

- coordinator and worker remain runtime responsibilities, not package boundaries
- Phase 1 may host those responsibilities inside one server binary
- distributed boundaries must still exist in code even when local execution is used

### Metadata split

- Lakekeeper-backed control-plane metadata remains outside BrewDB runtime ownership
- BrewDB runtime metadata is a separate logical store
- format-native metadata remains adapter-owned truth
- object storage remains artifact and data truth

### Planning split

- SQL intent planning in `brewdb-sql`
- orchestration planning in `brewdb-runtime`
- physical and stage planning in `brewdb-execution`

### Commit split

- workers may produce artifact-bearing outputs
- only the coordinator-side kernel may advance commit state
- only storage adapters may interpret and publish format truth

### Adapter split

- one table-level adapter per table format route
- adapters may hide sub-components internally
- upper layers must not bind directly to format-specific metadata models

## 14. Architecture Freeze Checklist

Before implementation starts, the development architecture should be considered frozen only if the following questions are answered with an explicit "yes" in review.

1. Are crate boundaries final enough to prevent runtime-role code sprawl?
2. Is the ownership split between `brewdb-runtime`, `brewdb-execution`, and `brewdb-storage` clear enough that commit, task execution, and format semantics will not mix?
3. Is `CatalogFacade` the only control-plane entry used by planning and commit flows?
4. Is the runtime metadata store explicitly separate in role from Lakekeeper?
5. Is the first walking skeleton agreed to be append-first rather than query-first?
6. Are worker outputs defined as non-authoritative staged artifacts rather than direct table-visible commits?
7. Are mutation and maintenance paths staying inside the same lifecycle framework instead of side tooling?

If any answer is "no", the architecture is still fluid and implementation should not begin.

## 15. Open Decisions To Lock Before Coding

The architecture is already strong on logical boundaries, but a few development-shaping decisions still need explicit confirmation.

### A. Workspace package layout

Recommended default:

- `crates/*` for shared libraries
- `bin/*` for binaries

Alternative:

- all packages under `crates/*`, including binaries

The main question is consistency, not capability. I recommend `crates/*` plus `bin/*` because it keeps product assembly visually separate from reusable kernels.

Decision recommendation:

- adopt `crates/*` plus `bin/*` as the Phase 1 repository convention

Why this should be the default:

- makes reusable kernels visually primary
- reduces the chance that server or tooling assembly code starts attracting shared logic
- keeps the workspace readable once integration tests and support packages appear

What this decision rules out:

- runtime-role packaging becoming the main mental model of the repository
- binaries quietly turning into ownership centers for domain logic

### B. RPC contract timing

Recommended default:

- do not freeze RPC schemas yet
- keep request/result contracts stable inside Rust modules first
- move wire-schema extraction later

This matches the existing Phase 1 bias against premature interface freeze.

Decision recommendation:

- do not freeze RPC or service IDL in early Phase 1

Why this should be the default:

- task/result contracts are still architecture-shaping, not just transport-shaping
- execution and commit boundaries need to settle before a durable wire schema is worth defending
- early IDL freeze would push accidental coupling into runtime internals

What this decision rules out:

- designing transport messages before execution contracts stabilize
- turning temporary local shapes into long-lived compatibility obligations

### C. Local execution mode

Recommended default:

- keep a local single-process development mode
- preserve runtime responsibility boundaries even in local mode

This reduces bring-up cost without collapsing the distributed design.

Decision recommendation:

- keep a first-class local single-process mode for development and integration testing

Why this should be the default:

- shortens bring-up and debug loops
- allows the walking skeleton to validate orchestration without waiting for full distributed deployment tooling
- preserves architecture truth as long as dispatch, task contract, and commit boundaries remain explicit in code

What this decision rules out:

- an in-process shortcut that bypasses task contracts
- folding execution runtime logic directly into orchestration modules

### D. Initial adapter target

Recommended default:

- make Paimon the first full adapter target
- keep Iceberg as a planned second adapter surface

That aligns with the existing Lakekeeper-native Paimon direction and avoids pretending both formats will mature at the same speed on day one.

Decision recommendation:

- treat Paimon as the first complete adapter target and Iceberg as interface-following Phase 1 scope

Why this should be the default:

- current catalog direction already favors Lakekeeper-native Paimon support
- one serious adapter is better for pressure-testing mutation, maintenance, and reconciliation boundaries than two partial adapters
- Iceberg can still shape generic adapter contracts without forcing equal implementation depth

What this decision rules out:

- false symmetry between Paimon and Iceberg in the first implementation wave
- delaying architecture validation until two adapters advance in parallel

## 16. Freeze Draft

If the project wants a concrete development-architecture freeze position today, the recommended Phase 1 answers are:

1. Repository layout: `crates/*` for shared libraries, `bin/*` for product-facing binaries such as `brewdbd` and `brewdb`
2. RPC timing: internal Rust contracts first, no early wire-schema freeze
3. Local mode: supported and encouraged, but must preserve runtime responsibility boundaries
4. Initial format target: Paimon first, Iceberg second

Together these choices optimize for boundary correctness over premature service decomposition.

They also fit the existing architecture constraints:

- capability-oriented crates stay primary
- transport remains secondary to execution and commit semantics
- distributed behavior is preserved even during local bring-up
- adapter abstractions are validated against one deep target before being generalized too aggressively

## 17. Approval Matrix

Use this matrix when you want to explicitly freeze the development architecture in review.

### Decision 1: repository layout

- recommended: approve `crates/*` + `bin/*`
- reject if you want binaries to live under `crates/*`
- effect of rejection: low architectural risk, mostly repository convention churn

### Decision 2: RPC timing

- recommended: approve late wire-schema freeze
- reject if you want IDL-first development
- effect of rejection: high coupling risk across runtime and execution contracts

### Decision 3: local execution mode

- recommended: approve first-class local mode with preserved boundaries
- reject if you want distributed-only bring-up
- effect of rejection: slower iteration and higher infrastructure cost early

### Decision 4: initial adapter target

- recommended: approve Paimon-first
- reject if you want equal Paimon/Iceberg depth from the start
- effect of rejection: slower adapter-kernel validation and wider early scope

## 18. Implementation Gate

Implementation should begin only after the four approval-matrix decisions above are either:

- explicitly approved as written, or
- replaced with alternate choices recorded in this document

Until then, this document should be treated as architecture work product, not implementation guidance only.

## 19. Design Rules

1. Crates are organized by stable kernel capability, not by deployment role.
2. Shared domain objects belong in `brewdb-core`; orchestration does not.
3. SQL frontend, lifecycle orchestration, execution, and storage semantics remain separate layers.
4. `brewdb-storage` owns storage semantics, while `brewdb-execution` owns execution runtime behavior.
5. Binaries assemble capabilities; they do not define capability boundaries.
