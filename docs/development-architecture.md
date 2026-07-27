# BrewDB Development Architecture

This document defines the Phase 1 development architecture for BrewDB. It translates the logical architecture into a crate/module structure for implementation. It does not define storage schemas, RPC wire formats, or Rust trait signatures.

## 1. Scope

Phase 1 implementation is organized as a monorepo with a small number of capability-oriented crates.

The crate layout is intentionally not based on deployment roles such as coordinator or worker. Binaries may be role-specific, but shared logic is organized by stable kernel boundaries.

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

### `brewdb-kernel`

Lifecycle and control kernel:

- job orchestration
- transaction orchestration
- commit orchestration
- lease handling
- recovery
- mutation orchestration
- maintenance orchestration

## 3. Dependency Direction

Recommended dependency direction:

- `brewdb-core`
- `brewdb-catalog -> brewdb-core`
- `brewdb-execution -> brewdb-core`
- `brewdb-storage -> brewdb-core + brewdb-catalog + selective brewdb-execution contracts`
- `brewdb-kernel -> brewdb-core + brewdb-catalog + brewdb-execution + brewdb-storage`
- `brewdb-sql -> brewdb-core + brewdb-catalog + brewdb-kernel`

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

### `brewdb-kernel::planning`

Holds:

- intent-to-execution orchestration planning
- lane timing and handoff planning
- mutation/maintenance job shaping
- bundle and commit handoff planning

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

### `brewdb-kernel`

Recommended modules:

- `jobs`
- `txns`
- `commit`
- `recovery`
- `leases`
- `mutation`
- `maintenance`
- `planning`
- `runtime`

## 6. Binary Assembly

Crates are capability-oriented, but binaries may still be role-oriented.

Phase 1 may expose binaries such as:

- `brewdb-coordinator`
- `brewdb-worker`

Those binaries should be assembly layers over shared crates. They should not define the crate structure.

## 7. Design Rules

1. Crates are organized by stable kernel capability, not by deployment role.
2. Shared domain objects belong in `brewdb-core`; orchestration does not.
3. SQL frontend, lifecycle orchestration, execution, and storage semantics remain separate layers.
4. `brewdb-storage` owns storage semantics, while `brewdb-execution` owns execution runtime behavior.
5. Binaries assemble capabilities; they do not define capability boundaries.
