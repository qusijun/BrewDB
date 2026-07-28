# BrewDB Coordinator CBO Optimizer Selection

This document defines the optimizer selection for the BrewDB coordinator. It focuses on query and mutation planning at the coordinator side, especially cost-based optimization (CBO) ownership, candidate evaluation, and Phase 1 implementation direction.

The baseline in this document is evaluated against the Rust and DataFusion ecosystem state as of July 28, 2026.

## 1. Goal

BrewDB needs a coordinator-side optimizer that can:

- produce good join order and join algorithm choices
- reason about distributed stage boundaries and data movement
- consume table and column statistics from catalog and storage adapters
- remain compatible with BrewDB's Rust-first architecture
- support query, insert-select, and later mutation / maintenance planning

The optimizer decision must not force BrewDB into a Java control plane, a split-language planner core, or a parallel metadata model that fights the storage adapters.

## 2. Decision

Phase 1 and Phase 2 baseline:

- use `Apache DataFusion` as the primary optimizer framework
- keep SQL frontend planning in `brewdb-sql`
- keep coordinator CBO ownership in `brewdb-runtime::planning`
- keep physical operator and stage shaping in `brewdb-execution`
- extend DataFusion with BrewDB-specific logical / physical optimizer rules instead of introducing a second optimizer kernel

This means BrewDB does **not** adopt Apache Calcite as the primary coordinator optimizer, and does **not** build a fully custom optimizer from scratch.

## 3. Why This Is The Right Fit

### 3.1 Rust and architecture fit

BrewDB is already organized as a Rust workspace with `brewdb-sql`, `brewdb-runtime`, and `brewdb-execution`.

Choosing DataFusion keeps:

- planner and executor in one language
- optimizer extensions in the same crate graph
- plan structures close to execution semantics
- lower operational complexity for debugging and versioning

Choosing Calcite would add a Java optimizer core into a Rust execution and runtime stack. That would introduce:

- cross-language plan translation
- duplicated type systems and function registries
- harder end-to-end debugging
- more fragile optimizer / execution contract evolution

### 3.2 DataFusion already has the optimizer hooks BrewDB needs

DataFusion already provides:

- logical optimizer rules
- physical optimizer rules
- statistics-driven join reordering
- join algorithm selection knobs
- custom `TableProvider` extension points
- pluggable statistics propagation through `StatisticsRegistry`

For BrewDB, that means we can reuse a mature optimizer skeleton and focus our effort on BrewDB-specific decisions:

- distributed exchange insertion
- object-store-aware scan shaping
- format-aware statistics ingestion
- commit-path planning for insert / mutation flows

### 3.3 CBO should stay close to execution reality

BrewDB's hardest planning problems are not only SQL rewrites. They are also:

- file and partition pruning
- exchange placement
- broadcast vs repartition tradeoffs
- sort preservation opportunities
- staged artifact materialization boundaries

These choices depend on execution-engine semantics. DataFusion keeps optimizer and execution contracts close enough that BrewDB can tune these choices without inventing a translation-heavy intermediate optimizer layer.

## 4. Candidate Evaluation

### Option A: DataFusion as the primary optimizer

Pros:

- native Rust fit
- already part of BrewDB's execution direction
- built-in logical and physical optimization pipeline
- built-in statistics-aware optimizer features
- easy extension through custom rules and providers
- no cross-language coordinator stack

Cons:

- distributed-system-level costing still needs BrewDB extensions
- mutation / maintenance optimization is not turnkey
- catalog and storage statistics normalization is BrewDB's responsibility

Assessment:

- best fit
- lowest architecture friction
- strongest path to an integrated coordinator CBO

### Option B: Apache Calcite as the primary optimizer

Pros:

- very mature rule / trait / CBO model
- proven in many analytical engines
- rich relational optimization framework

Cons:

- Java-first integration cost
- duplicated planner semantics against Rust execution
- more difficult UDF / type / function consistency
- more difficult physical-operator parity with DataFusion execution
- forces a plan interchange boundary much earlier than BrewDB needs

Assessment:

- technically strong optimizer
- strategically wrong fit for BrewDB Phase 1 and Phase 2

### Option C: fully custom BrewDB optimizer

Pros:

- complete control over semantics
- no external optimizer constraints

Cons:

- highest build cost
- reinvents mature optimizer infrastructure
- slower path to useful query quality
- highest correctness risk

Assessment:

- not justified at current stage

## 5. Coordinator Ownership Split

The coordinator optimizer should be split across three layers.

### `brewdb-sql`

Owns:

- SQL parsing
- binding
- semantic rewrites
- statement classification
- frontend intent generation

Must not own:

- global join costing
- distributed exchange placement
- stage graph generation

### `brewdb-runtime::planning`

Owns:

- coordinator-visible CBO policy
- planning session assembly
- statistics acquisition and normalization orchestration
- lane and commit-path planning
- dispatch-facing plan packaging

Must not own:

- low-level physical operator implementations
- storage-format truth
- worker-local execution decisions

### `brewdb-execution`

Owns:

- DataFusion physical planning integration
- stage graph generation
- partitioning / exchange shaping
- execution-operator-specific costing extensions

Must not own:

- job lifecycle truth
- commit orchestration
- SQL-surface semantics

## 6. What "Coordinator CBO" Means In BrewDB

In BrewDB, coordinator CBO is not a second full optimizer that competes with DataFusion. It is the coordinator-side decision framework that combines:

- DataFusion logical and physical optimization
- BrewDB statistics inputs
- BrewDB distributed planning policies
- BrewDB storage-adapter capabilities

Coordinator CBO makes or constrains decisions in five areas:

1. join order
2. join algorithm preference
3. scan pruning and scan shape
4. repartition / broadcast / exchange boundaries
5. materialization and commit handoff boundaries

The first two are mostly DataFusion-native with BrewDB statistics help.

The latter three are where BrewDB-specific policy matters most.

## 7. Statistics Model

The optimizer choice is only useful if BrewDB can feed it good statistics.

### 7.1 Statistics sources

The coordinator should merge statistics from:

- Lakekeeper-resolved table metadata
- format-native metadata from storage adapters
- object/file-level statistics when cheaply available
- BrewDB runtime observations from previous executions

### 7.2 Statistics ownership

Ownership should remain split:

- `brewdb-catalog`: table identity, route, warehouse profile, normalized metadata access
- `brewdb-storage`: format-aware table/file/partition statistics extraction
- `brewdb-runtime`: runtime feedback and plan-time statistics assembly

### 7.3 Statistics shape

Phase 1 should standardize these minimum inputs:

- row count
- total bytes
- per-column null count when available
- min/max bounds when available
- approximate NDV when available
- partition/file cardinality summaries
- sort / clustering hints when format metadata can expose them

### 7.4 Runtime feedback

The coordinator should later persist execution observations such as:

- actual output rows
- filter selectivity
- join build/probe sizes
- spill indicators
- skew indicators

This feedback should improve future planning, but it is not required to block the initial optimizer selection.

## 8. Chosen Technical Shape

### 8.1 Optimizer kernel

Use DataFusion's optimizer framework as the kernel:

- built-in logical optimizer rules remain enabled
- built-in physical optimizer rules remain enabled
- BrewDB adds custom rules around distribution and table-format-aware planning

### 8.2 Plan flow

Recommended flow:

1. `brewdb-sql` parses SQL and builds BrewDB intent
2. `brewdb-runtime::planning` opens a planning session and resolves catalog / adapter context
3. `brewdb-storage` provides scan and statistics inputs for referenced tables
4. DataFusion logical optimization runs with BrewDB-aware table providers and statistics
5. DataFusion physical optimization runs with BrewDB configuration and custom rules
6. `brewdb-execution` converts the physical plan into a BrewDB `StageGraph`
7. `brewdb-runtime::planning` wraps the result into an orchestration plan

### 8.3 Extension points BrewDB should use

BrewDB should extend DataFusion through:

- custom `TableProvider`
- custom optimizer rules
- custom physical optimizer rules
- pluggable statistics propagation
- session-level optimizer configuration

BrewDB should avoid:

- forking DataFusion optimizer internals too early
- embedding a parallel relational algebra model for the same query path

## 9. Join and Distribution Policy

The baseline policy should be conservative and explicit.

### 9.1 Join order

Primary baseline:

- use DataFusion statistics-driven join reordering
- only enable it when table statistics quality passes a minimum confidence threshold

Fallback:

- if statistics are weak or absent, preserve SQL join order or only apply very conservative local rewrites

### 9.2 Join algorithm

Primary baseline:

- let DataFusion choose between hash-join and sort-merge-oriented paths using statistics and configuration

BrewDB overlay policy:

- prefer broadcast-style behavior only when the build side is clearly bounded
- prefer repartitioned joins when table sizes or skew make broadcast unsafe
- downgrade aggressive hash-join preference when memory pressure history suggests spill risk

### 9.3 Exchange and stage boundaries

This remains a BrewDB-controlled layer above raw operator selection.

Coordinator policy should decide:

- where repartition becomes a distributed boundary
- whether scan locality or existing ordering should be preserved
- whether final materialization should happen in a dedicated stage

## 10. Non-Query Workloads

The optimizer selection must also support non-SELECT workflows.

### Insert-Select

Use the same DataFusion-based optimization path for the query-producing side, then hand off to BrewDB commit planning.

### Delete / Update / Merge

Phase 1 direction:

- use DataFusion as the query and row-selection optimizer
- keep mutation rewrite and commit semantics in BrewDB storage/runtime layers

This means the mutation framework can reuse the same optimizer kernel without pretending DataFusion owns snapshot commit truth.

### Maintenance

For compaction, rewrite, and analyze:

- use lighter-weight BrewDB planning when the workflow is not SQL-shaped
- reuse DataFusion when there is a real scan/filter/project/aggregate optimization problem

## 11. Phase Plan

### Phase 1

Adopt:

- DataFusion as optimizer kernel
- built-in logical and physical optimizer pipelines
- BrewDB statistics normalization shell
- minimal custom rules only where distributed stage shaping requires them

Do not adopt yet:

- a second optimizer framework
- adaptive re-optimization
- runtime-driven dynamic join switching
- a standalone plan service outside the coordinator

### Phase 2

Add:

- statistics confidence scoring
- runtime feedback persistence
- BrewDB-specific join/distribution costing
- stronger skew-aware planning
- better mutation-plan optimization

### Phase 3

Possible future additions:

- adaptive execution adjustments
- Substrait export/import boundary where interop becomes necessary
- optional externalized planning service if scale or product shape later demands it

## 12. Rejected Directions

### Primary Calcite-based coordinator optimizer

Rejected because:

- it breaks the Rust-first planner / executor cohesion
- it introduces a large cross-language maintenance surface
- it solves a maturity problem BrewDB does not currently have
- it would slow down execution-contract evolution

### Full custom optimizer before product fit

Rejected because:

- it spends effort on optimizer infrastructure instead of BrewDB-specific semantics
- it increases correctness and iteration risk

## 13. Final Recommendation

BrewDB should standardize on the following statement:

- the BrewDB coordinator uses DataFusion as its primary optimizer kernel
- BrewDB CBO is implemented as coordinator-owned planning policy plus DataFusion-native optimization
- BrewDB extends DataFusion through statistics, table providers, and custom rules instead of introducing Calcite or a second optimizer core

This is the best fit for:

- current Rust architecture
- distributed execution direction
- multi-format storage support
- low-friction Phase 1 delivery
- long-term evolution toward stronger distributed CBO

## 14. Implementation Landing Zones

Current crate landing zones:

- `brewdb-sql`
  - SQL AST, bind, analyze, rewrite, intent
- `brewdb-runtime::planning`
  - planning session
  - statistics assembly
  - optimizer policy
  - orchestration plan packaging
- `brewdb-execution`
  - DataFusion integration
  - physical planning
  - stage graph generation
- `brewdb-storage`
  - statistics extraction
  - scan capability reporting
  - commit / mutation semantics

This split should be treated as the implementation baseline unless a later architecture review explicitly overturns it.
