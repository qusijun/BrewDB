# BrewDB Maintenance Kernel Phase 1

This document defines the Phase 1 maintenance kernel for BrewDB. It covers maintenance job categories, execution shape, maintenance artifacts, and conflict/validation boundaries. It does not define UI, scheduling policy details, or long-task resumability.

## 1. Scope

Phase 1 maintenance includes:

- `COMPACT`
- `REWRITE`
- `ANALYZE`
- `CLEANUP`

Maintenance is a first-class kernel subsystem. It is not treated as an external script layer.

## 2. Maintenance Job Families

Phase 1 uses three maintenance families.

### `RewriteMaintenance`

Includes:

- `COMPACT`
- `REWRITE`

Characteristics:

- candidate scope must first be selected
- distributed rewrite may be required
- staged rewrite artifacts are produced
- finalization typically uses transaction and commit flow

### `MetadataMaintenance`

Includes:

- `ANALYZE`

Characteristics:

- reads table or table-derived state
- produces statistics or metadata summaries
- may publish table-level metadata through a lightweight finalize path

### `CleanupMaintenance`

Includes:

- `CLEANUP`

Characteristics:

- discovers staged, orphaned, or obsolete objects
- validates that they are no longer live
- finalizes through controlled deletion rather than data rewrite

## 3. Shared Maintenance Shape

All Phase 1 maintenance jobs follow the same high-level shape:

1. `Select`
2. `Act`
3. `Validate`
4. `Finalize`

### `Select`

Identify the maintenance scope:

- compact/rewrite candidates
- analyze target scope
- cleanup candidate scope

### `Act`

Execute the job's main work:

- rewrite data
- collect statistics
- discover or prepare deletions

### `Validate`

Confirm the selected scope is still valid:

- table state remains acceptable
- artifacts are complete
- cleanup targets are no longer referenced

### `Finalize`

Apply the maintenance result:

- publish rewritten state
- publish statistics metadata
- delete orphaned objects

## 4. Rewrite-Producing Maintenance

`COMPACT` and `REWRITE` are modeled as maintenance jobs with:

- a `SelectionBoundary`
- a distributed rewrite phase
- a staged rewrite artifact bundle
- a maintenance-lane critical section
- transaction + commit finalization

These jobs are similar to mutation in execution shape, but their semantics are object/layout rewrite semantics rather than row-set mutation semantics.

## 5. Analyze as Maintenance

`ANALYZE` is modeled as:

- scope selection
- distributed statistics collection
- statistics artifact production
- optional lightweight metadata publish

Phase 1 treats `ANALYZE` as maintenance even when it looks execution-wise like a query. If statistics are written back, the finalize path is maintenance-aware rather than query-like.

## 6. Cleanup as Maintenance

`CLEANUP` is modeled as:

- discovery
- validation
- deletion finalize

Cleanup must validate that targets:

- are not referenced by live jobs
- are not referenced by live transactions
- are not needed by unknown-outcome commit attempts
- do not remain part of current external format truth

Cleanup is governed by runtime truth and external truth, not by ad hoc deletion scripts.

## 7. Maintenance Artifacts

Phase 1 maintenance recognizes three main artifact kinds.

### `CandidateSetArtifact`

Used for:

- compact/rewrite selection output

### `RewriteArtifactBundle`

Used for:

- compact/rewrite staged publish candidates

### `StatsArtifact` / `CleanupArtifact`

Used for:

- analyze outputs
- cleanup targets or cleanup execution records

## 8. Maintenance Coordination

Rewrite-producing maintenance uses the table `maintenance` lane in critical sections.

### `COMPACT` / `REWRITE`

- must use `maintenance` lane before final rewrite-sensitive publish work

### `ANALYZE`

- may run without table-scoped exclusivity while collecting statistics
- if it writes shared table metadata, it enters a lightweight maintenance finalize path

### `CLEANUP`

- is primarily governed by cluster-level housekeeping coordination
- may require table-aware validation before deleting table-related objects

## 9. Conflict Model

Phase 1 uses conservative conflict handling.

### Rewrite maintenance

Conflicts with:

- `mutation`
- `ddl`
- other rewrite-producing maintenance

### Metadata maintenance

- may run read-only without table exclusivity
- must coordinate when publishing metadata

### Cleanup maintenance

- must avoid interfering with live jobs and unresolved transactions
- relies more on truth validation than on broad table locking

## 10. Adapter Boundary

The maintenance kernel owns:

- maintenance job lifecycle
- selection/execution/finalize orchestration
- maintenance artifacts
- maintenance lane usage
- transaction and commit shell when needed

Format adapters own:

- candidate-set contract
- rewrite contract
- maintenance validation semantics
- statistics publish semantics
- cleanup truth checks when format-aware validation is required

## 11. Design Rules

1. Maintenance is a first-class kernel subsystem.
2. Phase 1 maintenance includes rewrite-producing, metadata-producing, and cleanup-style jobs.
3. All maintenance jobs follow a shared shape: select, act, validate, finalize.
4. Rewrite maintenance uses selection boundaries and staged rewrite artifacts.
5. Analyze is modeled as maintenance with a light publish path when stats are written back.
6. Cleanup is governed by runtime truth and external truth, not by uncontrolled deletion logic.
