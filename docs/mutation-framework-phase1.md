# BrewDB Mutation Framework Phase 1

This document defines the Phase 1 mutation framework for BrewDB. It covers mutation categories, logical mutation shape, execution artifacts, and adapter boundaries. It does not include `MERGE INTO`.

## 1. Scope

Phase 1 mutation supports:

- `INSERT`
- `INSERT SELECT`
- `DELETE`
- `UPDATE`

Phase 1 does not support:

- `MERGE INTO`
- join-based `DELETE`
- join-based `UPDATE`
- correlated mutation subqueries as first-class mutation paths

## 2. Mutation Families

BrewDB uses two mutation families.

### Append-like

- `INSERT`
- `INSERT SELECT`

Characteristics:

- input is new rows
- target-table old rows are not matched
- output is staged append data

### Rewrite-like

- `DELETE`
- `UPDATE`

Characteristics:

- target-table rows must be scanned
- a matched target row-set must be identified
- row-level mutation effects must be produced
- physical realization is format-specific

## 3. Logical Mutation Model

Rewrite-like mutation is modeled as:

- `matched row-set`
- `row transform`

### `DELETE`

- match target rows with the predicate
- transform each matched row into `DropRow`

### `UPDATE`

- match target rows with the predicate
- transform each matched row into `ReplaceRow`

`UPDATE` is not lowered directly to `delete + insert` in the BrewDB logical layer. It remains a row replacement intent until a format adapter chooses a physical strategy.

## 4. Planner Output

`DELETE` and `UPDATE` compile into a BrewDB-native rewrite mutation plan.

The plan includes:

- target table binding
- target schema revision
- predicate
- match scope hint
- row effect

Examples:

- `DELETE` -> `row_effect = DropRow`
- `UPDATE` -> `row_effect = ReplaceRow(assignments...)`

The planner output is not a format-native commit request.

## 5. Execution Skeleton

### Append-like execution

1. build source plan
2. normalize rows to target schema
3. repartition / sort / cluster as needed
4. write staged data artifacts
5. aggregate an append artifact bundle
6. enter the `mutation` lane
7. create txn and commit

### Rewrite-like execution

1. build target scan plan
2. match the target row-set
3. apply row transform
4. materialize mutation effects
5. write staged mutation artifacts
6. aggregate a rewrite artifact bundle
7. enter the `mutation` lane
8. create txn and commit

`COMPACT` is not part of the mutation framework. It remains a maintenance job, even though it shares execution and commit structure.

## 6. Artifact Model

Mutation execution produces task/stage-level manifests and job-level artifact bundles.

### Artifact manifests

Produced by execution paths to record:

- staged artifact references
- task/stage origin
- basic artifact metadata

### Artifact bundles

Aggregated by commit orchestration to represent commit candidates.

#### Append-like bundle

Captures:

- staged append artifacts
- partition/bucket summary
- row-count and size summaries
- schema revision

#### Rewrite-like bundle

Captures:

- base snapshot or version
- mutation kind
- matched-scope summary
- effect summary
- affected-object summary
- staged mutation artifacts
- schema revision

Artifact bundles are BrewDB-native commit inputs, not raw format-native commit requests.

## 7. Adapter Responsibilities

Format adapters own mutation semantics below the BrewDB logical layer.

They are responsible for:

- capability decisions
- mutation materialization strategy
- commit validation requirements
- publish semantics

They are not responsible for:

- SQL planning
- job scheduling
- runtime-state management
- lease management
- commit journaling

## 8. Rewrite Realization

Adapters consume the BrewDB rewrite mutation plan and return a format-aware realization plan.

That realization plan defines:

- physical strategy kind
- required scan shape
- effect materialization mode
- artifact contract
- validation scope hint

Execution uses the realization plan to produce correct staged artifacts. Commit uses it to validate and publish those artifacts safely.

## 9. Planner and Adapter Boundary

BrewDB owns:

- mutation intent
- distributed execution
- job lifecycle
- transaction lifecycle
- commit orchestration

Format adapters own:

- format-aware mutation realization
- validation semantics
- format-native publish behavior

This keeps mutation intent unified while preserving format-specific physical behavior.

## 10. Design Rules

1. Phase 1 mutation has two families: append-like and rewrite-like.
2. `DELETE` and `UPDATE` are rewrite-like mutations.
3. Rewrite-like mutation is expressed as `matched row-set + row transform`.
4. `DELETE` maps matched rows to `DropRow`; `UPDATE` maps matched rows to `ReplaceRow`.
5. Artifact bundles are BrewDB-native commit inputs.
6. Format adapters decide physical realization and publish semantics.
