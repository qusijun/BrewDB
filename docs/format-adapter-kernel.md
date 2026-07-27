# BrewDB Format Adapter Kernel

This document defines the format adapter kernel in BrewDB. It covers the table-level adapter boundary, the semantic surfaces exposed by adapters, and the division of responsibility between adapters and the rest of the kernel. It does not define format-specific implementation details.

## 1. Scope

BrewDB integrates table formats through table-level adapters.

Examples:

- `PaimonTableAdapter`
- `IcebergTableAdapter`

The adapter kernel is responsible for format-facing read, write, mutation, maintenance, commit, and reconciliation semantics.

It is not responsible for:

- catalog control-plane access
- job scheduling
- runtime-state ownership
- transaction journaling
- SQL parsing or binding

## 2. Table-Level Adapter Boundary

The primary integration unit is a table-level adapter rather than a set of unrelated per-capability adapters.

This keeps the following semantics aligned inside one boundary:

- scan behavior
- append behavior
- rewrite mutation behavior
- maintenance behavior
- commit and reconciliation behavior

Adapters may be internally decomposed by capability, but that decomposition stays hidden behind the table-level boundary.

## 3. Adapter Inputs

The table adapter primarily consumes:

- a BrewDB table envelope from the catalog layer
- a format handle pointing at format truth
- an operation context for scan, append, rewrite mutation, maintenance, commit, or reconciliation

The adapter does not consume raw SQL as its main input.

## 4. Adapter Outputs

Adapters mainly provide:

- capability views
- realization and contract information for execution
- validation and publish behavior for commit
- truth resolution behavior for reconciliation

They do not return final job lifecycle outcomes or runtime-store ownership decisions.

## 5. Semantic Surfaces

Each table adapter exposes five semantic surfaces.

### `Scan Surface`

Defines:

- scan capabilities
- snapshot binding mode
- required row shape
- layout hints
- scan-time validation requirements

This surface tells the planner and execution framework how the table may be read.

### `Append Surface`

Defines:

- append support
- input requirements for append-like mutations
- staged append artifact contract
- append validation scope
- append publish mode

This surface governs `INSERT` and `INSERT SELECT`.

### `Rewrite Mutation Surface`

Defines:

- rewrite mutation capabilities
- required scan shape for `DELETE` and `UPDATE`
- realization strategy space
- row-effect materialization contract
- validation scope hints
- conflict sensitivity

This surface governs how logical row effects become format-aware mutation actions.

### `Maintenance Surface`

Defines:

- maintenance capabilities
- candidate-selection contract
- rewrite contract
- validation scope hints
- maintenance conflict model

This surface governs compact/rewrite/analyze-style maintenance behavior.

### `Commit / Reconciliation Surface`

Defines:

- commit binding requirements
- validation rules
- publish contract
- commit result shape
- reconciliation source of truth
- truth resolution model

This surface connects BrewDB transaction state to external format truth.

## 6. Responsibility Split

### BrewDB core owns

- job lifecycle
- distributed execution
- transaction lifecycle
- commit orchestration
- runtime-state persistence

### `CatalogFacade` owns

- Lakekeeper access
- control-plane identity
- normalized table envelopes
- format routing and handles

### Table adapters own

- format truth interpretation
- mutation and maintenance realization
- validation semantics
- publish semantics
- reconciliation semantics

## 7. Planner / Execution / Commit Interaction

### Planner

Uses adapters for:

- capability checks
- scan requirements
- mutation and maintenance realization hints

The planner does not own format-native semantics.

### Execution framework

Uses adapters for:

- scan shape requirements
- artifact contracts
- effect materialization requirements

Execution produces staged outputs but does not decide final external visibility.

### Commit manager

Uses adapters for:

- validation rules
- publish behavior
- reconciliation truth lookup

The commit manager owns the orchestration shell, while adapters own format semantics.

## 8. Design Rules

1. BrewDB uses table-level adapters as its primary format integration unit.
2. Table adapters own scan, mutation, maintenance, commit, and reconciliation semantics for their format.
3. CatalogFacade provides control-plane identity and handles; adapters provide format truth and behavior.
4. Adapters may be internally decomposed, but that decomposition remains hidden behind the table-level boundary.
5. Planner, execution, and commit logic stay unified in BrewDB even when format semantics differ underneath.
