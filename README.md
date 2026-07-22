# BrewDB

BrewDB is a distributed lakehouse database engine for multiple table formats.

It is not positioned as a query-only SQL layer. The target shape is closer to a ClickHouse-style data engine built on top of external table formats such as Paimon and Iceberg, with first-class support for:

- query
- insert / insert select
- mutation
- compaction / rewrite
- analyze / cleanup
- background maintenance

## Positioning

BrewDB is organized around three planes:

- `Control Plane`: Lakekeeper plus BrewDB extensions for catalog, auth, warehouse, and governance
- `Execution Plane`: DataFusion plus BrewDB distributed runtime for query, mutation, and maintenance execution
- `Storage Semantics Plane`: format-specific adapters such as Paimon and Iceberg for snapshot, commit, conflict detection, and maintenance rules

The system goal is unified execution and lifecycle orchestration, not forced unification of format-native transaction semantics.

## Architecture Summary

### Complete Architecture

```text
+-----------------------------------------------------------------------------------+
| Clients / Interfaces                                                              |
| SQL / HTTP API / admin operations                                                 |
+------------------------------------------+----------------------------------------+
                                           |
                                           v
+-----------------------------------------------------------------------------------+
| BrewDB Coordinator                                                                |
| - SQL frontend                                                                    |
| - query / mutation / maintenance planner                                          |
| - distributed scheduler                                                           |
| - transaction and commit coordinator                                              |
| - recovery manager                                                                |
+---------------------------+--------------------------+----------------------------+
                            |                          |
                            | catalog / auth / creds   | runtime state / recovery
                            v                          v
        +--------------------------------+   +--------------------------------------+
        | Lakekeeper + BrewDB extensions |   | BrewDB Runtime Store                 |
        | - namespace / table identity   |   | - jobs / stages / task attempts      |
        | - ACL / governance             |   | - txn intents / commit journal       |
        | - warehouse / credentials      |   | - staged artifact manifests          |
        | - native Paimon catalog        |   | - recovery checkpoints               |
        +----------------+---------------+   +------------------+-------------------+
                         |                                      |
                         v                                      v
              +------------------------+             +------------------------+
              | PostgreSQL             |             | PostgreSQL             |
              | control-plane store    |             | runtime metadata store |
              +------------------------+             +------------------------+
                                          
                                          
+-----------------------------------------------------------------------------------+
| Distributed Execution Plane                                                       |
| DataFusion + BrewDB runtime                                                       |
| - stage/task execution                                                            |
| - exchange / shuffle                                                              |
| - query / insert / mutation compute / compaction                                  |
+-----------------------------+-------------------------------+---------------------+
                              |                               |
                              v                               v
                   +--------------------+          +--------------------+
                   | Worker             |          | Worker             |
                   | - fragment exec    |          | - fragment exec    |
                   | - local spill      |          | - local spill      |
                   | - staged outputs   |          | - staged outputs   |
                   +---------+----------+          +----------+---------+
                             \                                /
                              \                              /
                               +----------------------------+
                               | Object Storage             |
                               | - table data               |
                               | - snapshots / manifests    |
                               | - staged write artifacts   |
                               +-------------+--------------+
                                             |
                                             v
                               +----------------------------+
                               | Format Semantics Layer     |
                               | - Paimon adapter           |
                               | - Iceberg adapter          |
                               | - future adapters          |
                               +----------------------------+
```

### Control Plane

Lakekeeper is the control-plane foundation. BrewDB extends it instead of introducing a separate catalog stack for each format.

Lakekeeper is expected to own:

- namespace, database, and table identity
- authn / authz
- warehouse and storage credential vending
- governance and audit
- routing to native catalog modules

For Paimon, the intended direction is native support in Lakekeeper rather than generic-table-only registration.

The current metadata backend choice for the control plane is `PostgreSQL`.

### Execution Plane

DataFusion is treated as the unified execution substrate, not only as a query engine.

The execution plane should eventually cover:

- distributed query execution
- insert and insert-select execution
- mutation compute paths for delete / update / merge
- compaction and rewrite jobs
- analyze and cleanup jobs

The current metadata backend choice for BrewDB runtime state is also `PostgreSQL`, kept logically separate from the control-plane store even if the first deployment shares one PostgreSQL instance.

### Storage Semantics Plane

Format adapters remain responsible for format-native truth:

- snapshot lineage
- metadata layout
- commit protocol
- optimistic conflict detection
- row-level mutation strategy
- maintenance correctness constraints

## Distributed Shape

BrewDB is intended to support distributed deployment from the start.

The topology is:

- a central coordinator for planning, scheduling, commit orchestration, and recovery
- multiple workers for DataFusion fragment execution and staged artifact generation
- object storage for table data and staged outputs
- Lakekeeper as the control-plane base
- PostgreSQL for Lakekeeper control-plane metadata
- a dedicated PostgreSQL-backed BrewDB runtime metadata store for jobs, transaction intents, and recovery state

This is a shared-storage distributed architecture with ClickHouse-style lifecycle ambitions, not a query-only coordinator model.

## Capability Boundaries

What BrewDB should unify:

- SQL surface
- job and task orchestration
- distributed execution
- lifecycle operations
- recovery skeleton
- observability

What BrewDB should not force into one abstraction:

- table-format metadata layout
- snapshot structure
- commit semantics
- conflict detection rules
- row-level change representation

## Metadata Ownership

Metadata is split across four domains:

1. `Lakekeeper`
   - control-plane and business metadata
2. `Format-native metadata`
   - Paimon / Iceberg catalog and format truth
3. `BrewDB runtime store`
   - jobs, task attempts, transaction intents, commit journal, recovery state
4. `Object storage`
   - data files, metadata objects, staged artifacts

Current storage decision:

- `Lakekeeper metadata store`: PostgreSQL
- `BrewDB runtime metadata store`: PostgreSQL
- first deployment may share one PostgreSQL instance, but with separate logical schemas or databases

This separation is a core architecture rule.

## Near-Term Scope

The current architecture direction favors:

- coordinator / worker separation from day one
- Lakekeeper-native Paimon support
- DataFusion as shared execution substrate
- mutation and maintenance as built-in system abilities
- centralized final commit with worker-generated staged outputs

Not first-priority:

- global TSO
- cross-format atomic transactions
- forced unification of all format-native transaction behavior

## Repository Docs

- [Architecture Constraints](docs/architecture-constraints.md)
