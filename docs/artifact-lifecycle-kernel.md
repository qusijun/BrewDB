# BrewDB Artifact Lifecycle Kernel

This document defines the artifact and object lifecycle kernel in BrewDB. It covers staged outputs, manifests, bundles, publish transitions, and reclaimability. It does not define object-store path layout or implementation-level schema details.

## 1. Scope

The artifact lifecycle kernel defines how execution outputs move from creation to final publication or cleanup.

It applies to:

- append-like mutation outputs
- rewrite-like mutation outputs
- maintenance candidate outputs
- maintenance rewrite outputs
- analyze outputs
- cleanup targets

## 2. Artifact Object Families

Phase 1 recognizes four logical object families.

### `StagedArtifact`

Worker-produced execution output that is not yet externally visible as table truth.

Examples:

- staged append data
- staged mutation artifacts
- staged rewrite outputs
- candidate-set artifacts
- stats artifacts

### `ArtifactManifest`

A structured runtime reference to staged artifacts.

It records:

- which task or stage produced the artifact
- where the artifact lives
- basic artifact metadata

### `ArtifactBundle`

A job- or commit-level aggregation of artifacts that are candidates for finalize/publish.

Examples:

- append bundle
- rewrite mutation bundle
- maintenance rewrite bundle
- stats bundle

### `PublishedObject`

An object that has become part of external format truth and must be governed by format semantics and retention rules rather than staged cleanup rules.

## 3. Lifecycle States

Artifacts move through five logical states:

- `produced`
- `registered`
- `bundled`
- `published`
- `reclaimable`

### `produced`

The artifact has been written by a worker but has not yet been registered into runtime truth.

### `registered`

The artifact is recorded by runtime metadata through manifests or similar references.

### `bundled`

The artifact is associated with a specific finalize candidate through an artifact bundle.

### `published`

The artifact has been accepted into external format truth.

### `reclaimable`

The artifact has been determined safe to clean up or reclaim.

## 4. Lifecycle Ownership

Different kernel areas own different state transitions.

### Execution kernel

Owns:

- `produced`

### Runtime metadata / aggregation paths

Own:

- `registered`
- `bundled`

### Commit + adapter path

Own:

- `published`

### Cleanup / housekeeping path

Own:

- `reclaimable`

## 5. Bundles as Finalize Boundaries

Bundles define the finalize/publish candidate boundary for artifacts.

They connect:

- execution-side outputs
- transaction/commit attempts
- cleanup and recovery reasoning

Bundles are BrewDB-native control objects. They are not equivalent to raw format-native commit requests.

## 6. Family-Specific Lifecycle Notes

### Append-like mutation

- produces staged append artifacts
- registers manifests
- aggregates an append bundle
- publishes selected artifacts through commit

### Rewrite-like mutation

- produces staged rewrite/delete/replacement artifacts
- registers manifests
- aggregates a rewrite mutation bundle
- publishes selected artifacts through commit

### Rewrite-producing maintenance

- produces candidate-set artifacts and rewrite artifacts
- aggregates maintenance bundles
- publishes rewritten state through finalize/commit

### Analyze

- produces stats artifacts
- may aggregate a stats bundle
- may publish metadata-level results

### Cleanup

- does not usually produce table-visible data
- identifies reclaimable objects
- finalizes by controlled deletion

## 7. Publish Is Not Object Existence

The existence of an object in object storage does not imply that it has been published.

Likewise:

- a manifest does not imply publish
- a bundle does not imply publish
- a failed job does not always imply reclaimability

External visibility begins only after commit/finalize succeeds and external format truth accepts the object.

## 8. Reclaimability Rules

An artifact is reclaimable only when all of the following are true:

- it is not referenced by a live job or task
- it is not referenced by a live bundle
- it is not required by an active transaction or commit attempt
- it is not blocked by an unknown-outcome transaction
- it is not part of current external format truth
- it is not retained by preservation or retention policy

Cleanup decisions must therefore depend on:

- runtime truth
- transaction truth
- external format truth
- retention policy

## 9. Unknown Outcome Handling

Artifacts associated with unknown-outcome transactions must not be aggressively reclaimed.

Until reconciliation completes, those artifacts may still be:

- part of a successful publish
- part of an aborted but not yet fully resolved attempt

Unknown-outcome transactions therefore block immediate cleanup of related artifacts.

## 10. Cleanup vs Retention

The lifecycle kernel distinguishes:

- `cleanup`: whether an artifact is fundamentally reclaimable
- `retention`: whether a reclaimable artifact should still be preserved for a period of time

Cleanup determines eligibility for reclamation. Retention determines timing.

## 11. Design Rules

1. Execution output existence does not imply publish visibility.
2. Artifacts move through produced, registered, bundled, published, or reclaimable states.
3. Bundles define commit/finalize candidate boundaries for artifacts.
4. Cleanup decisions depend jointly on runtime truth, transaction truth, and external format truth.
5. Unknown-outcome transactions block aggressive artifact reclamation until reconciliation completes.
