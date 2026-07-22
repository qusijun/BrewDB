# BrewDB Architecture Constraints

This document records the current architecture decisions and capability boundaries for BrewDB. It is intentionally high level and is meant to constrain future design work before detailed interfaces are introduced.

## 1. System Identity

BrewDB is a distributed lakehouse database engine for multiple table formats.

It should not be designed as:

- a query-only SQL layer
- a thin wrapper around DataFusion
- a separate sidecar toolchain for ingest, mutation, and maintenance

It should be designed as a unified data lifecycle engine covering:

- query
- insert / insert select
- delete / update / merge
- compaction / rewrite
- analyze / cleanup
- background maintenance

The intended capability profile is closer to ClickHouse-style lifecycle completeness than to Presto-style query specialization, while relying on external table formats instead of a self-owned storage engine.

## 2. Core Architecture Planes

### 2.1 Control Plane

The control plane is built on Lakekeeper plus BrewDB extensions.

Responsibilities:

- namespace, database, and table identity
- authn / authz
- warehouse and storage profile management
- credential vending
- governance and audit
- catalog module routing for multiple formats

### 2.2 Execution Plane

The execution plane is built on DataFusion plus BrewDB distributed runtime components.

Responsibilities:

- query execution
- insert and insert-select execution
- mutation compute
- compaction and rewrite execution
- analyze and cleanup execution
- distributed stage/task scheduling

### 2.3 Storage Semantics Plane

The storage semantics plane is implemented by format adapters such as Paimon and Iceberg.

Responsibilities:

- snapshot binding
- metadata interpretation
- commit protocol
- conflict detection
- mutation physical strategy
- maintenance correctness constraints

## 3. DataFusion Positioning

DataFusion is not only a query engine inside BrewDB.

It is the unified execution substrate for:

- reads
- writes
- mutation compute
- maintenance jobs

BrewDB should extend DataFusion around distributed planning and execution rather than build a parallel execution engine for non-query workflows.

At the same time, BrewDB should not force table-format semantics into DataFusion itself. Execution is unified; format truth remains format-specific.

## 4. Catalog Direction

Lakekeeper is the control-plane foundation and should be extended instead of bypassed.

For Paimon specifically, the direction is:

- do not introduce a standalone parallel Paimon catalog service outside the control plane
- do not stop at generic-table registration only
- extend Lakekeeper with native Paimon catalog support

This means BrewDB is not avoiding catalog implementation work; it is choosing to place that work inside the shared control-plane base.

## 5. Distributed Topology

BrewDB must be architected for distributed deployment.

The intended topology is:

- coordinator
- workers
- object storage
- Lakekeeper control plane
- BrewDB runtime metadata store

### 5.1 Coordinator

The coordinator is responsible for:

- SQL/API entry
- planning
- job orchestration
- transaction intent handling
- staged output aggregation
- final commit triggering
- recovery
- maintenance scheduling

The coordinator is not the long-term source of catalog truth.

### 5.2 Workers

Workers are responsible for:

- DataFusion fragment execution
- scan, join, aggregate, sort, repartition
- staged output generation
- metrics and artifact reporting

Workers should not own authoritative table metadata or final commit authority.

## 6. Runtime Metadata Requirement

BrewDB needs its own runtime metadata store.

This must be separate in role from Lakekeeper, even if both initially share the same physical database instance.

The runtime store is expected to hold:

- job state
- stage and task-attempt state
- transaction intents
- staged artifact manifests
- commit journal
- recovery checkpoints
- maintenance leases and progress

Anything the system needs in order to recover after coordinator failure belongs here.

## 7. Metadata Ownership Boundaries

Metadata ownership is split across four domains.

### 7.1 Lakekeeper

Owns control-plane metadata:

- identity
- namespaces and logical tables
- ACL / RBAC
- warehouse and storage profile
- governance and audit
- native catalog entry points

### 7.2 Format-Native Metadata

Owns format truth:

- snapshot lineage
- schema evolution history
- manifest or metadata-tree structure
- row-level mutation semantics
- final commit semantics

BrewDB should not create a competing source of truth for these details.

### 7.3 BrewDB Runtime Metadata

Owns execution and recovery truth:

- jobs
- task attempts
- transaction intents
- staged outputs
- commit attempts
- recovery state

### 7.4 Object Storage

Owns:

- table data files
- metadata objects
- staged outputs
- temporary rewrite artifacts

## 8. Mutation and Maintenance Are First-Class

BrewDB must treat mutation and maintenance as built-in system capabilities, not side tools.

This includes:

- delete
- update
- merge into
- compaction
- rewrite
- analyze
- cleanup

They should share the same job/task execution framework as query and insert flows.

## 9. Transaction Positioning

BrewDB should implement a unified transaction coordination framework, not a single forced transaction model for all formats.

Unified at the BrewDB layer:

- transaction lifecycle orchestration
- intent logging
- idempotency
- commit coordination shell
- recovery

Format-specific:

- snapshot publishing
- metadata mutation
- conflict detection
- validation details

At the current stage, a TSO service is not a required foundation. Recovery, idempotent publish, and staged artifact tracking are higher-priority concerns.

## 10. Capability Boundaries

BrewDB should unify:

- SQL surface
- lifecycle orchestration
- distributed execution
- scheduler model
- recovery framework
- observability

BrewDB should not force-compress:

- snapshot models
- metadata layouts
- conflict rules
- mutation encodings
- final commit semantics

This boundary is fundamental to sustainable multi-format support.

## 11. Current Phase Direction

The present architecture direction favors:

- coordinator / worker separation from day one
- native Paimon support through Lakekeeper extensions
- DataFusion as shared execution substrate
- a dedicated BrewDB runtime metadata store
- centralized final commit after distributed staged-output generation
- mutation and maintenance as built-in framework capabilities

The following are intentionally not early commitments:

- global TSO
- cross-format atomic transactions
- full semantic unification across formats
- premature detailed interface design before architecture is stable

## 12. Design Guardrails

Future design work should be evaluated against these rules:

1. Does this reduce or increase sidecar behavior outside the main lifecycle engine?
2. Does this preserve clear metadata ownership boundaries?
3. Does this keep DataFusion as the unified execution substrate without overloading it with format truth?
4. Does this let Lakekeeper remain the control-plane foundation?
5. Does this keep workers free of authoritative commit responsibility?
6. Does this improve distributed recovery instead of only improving the happy path?
