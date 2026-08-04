# BrewDB

BrewDB is a distributed MPP lakehouse database engine built around a BrewDB-owned catalog, DataFusion-based planning and execution, and format-aware storage engines such as Paimon and Iceberg.

The current repository state is intentionally architecture-first:

- design documents are the source of truth
- code has been reset to skeleton layout
- implementation will be rebuilt from the documented crate boundaries

## Top-Level Shape

The main product entrypoints are:

- `brewdb`: SQL client
- `brewdbd`: server process host

The main query path is:

`brewdb -> brewdbd -> brewdb-frontend -> brewdb-sql -> brewdb-planner -> brewdb-runtime -> brewdb-execution`

## Architecture Diagram

```text
+-----------+      +---------+      +------------------+      +------------+
|  brewdb   | ---> | brewdbd | ---> | brewdb-frontend  | ---> | brewdb-sql |
+-----------+      +---------+      +------------------+      +------------+
                                                                  |
                                                                  v
                                                           +---------------+
                                                           | brewdb-planner|
                                                           +---------------+
                                                                  |
                                                                  v
                   +------------------+                   +---------------+                   +------------------+
                   |  brewdb-catalog  | <---------------  | brewdb-runtime| ---------------> | brewdb-execution |
                   +------------------+                   +---------------+                   +------------------+
                           |                                      |                                      |
                           v                                      v                                      v
                   +------------------+                   +------------------+                   +------------------+
                   | CatalogMeta/FDB  |                   | RuntimeMeta/FDB  |                   | Arrow/DataFusion |
                   +------------------+                   +------------------+                   +------------------+
                           |                                                                             |
                           +-----------------------------------+-----------------------------------------+
                                                               |
                                                               v
                                                       +----------------+
                                                       | brewdb-storage |
                                                       +----------------+
                                                               |
                                                               v
                                         +---------------------------------------------+
                                         | TableEngine implementations                 |
                                         | - PaimonTableEngine                        |
                                         | - IcebergTableEngine                       |
                                         +---------------------------------------------+
```

Read path:

`brewdb -> brewdbd -> frontend -> sql -> planner -> runtime -> execution`

Metadata and storage side paths:

- `sql / planner / runtime -> brewdb-catalog`
- `runtime / execution -> brewdb-storage`
- `runtime -> RuntimeMeta`

## Crate Layout

Phase 1 is organized around capability-oriented crates rather than coordinator/worker repository splits.

- `brewdb-common`
  Shared common infrastructure and foundational components. This crate replaces the old `brewdb-core` role and now focuses on logger bootstrap, structured event helpers, diagnostics/error-code primitives, job-config layering primitives with explicit `system < session < statement` precedence, a registry-backed config whitelist for `brewdb.*` keys, and other low-level reusable building blocks rather than a large domain-kernel grab bag.
- `brewdb-catalog`
  BrewDB-owned catalog metadata kernel. Owns the `catalog.database.table` hierarchy, `Path / Ref / Entry` model, `CatalogService`, and the `CatalogStore / CatalogStoreBackend` split. The catalog store keeps control-plane identity plus table-location bindings, while format-native schema and snapshot truth stay below the lake-format metadata boundary.
- `brewdb-frontend`
  Session ingress and client-facing protocol boundary.
- `brewdb-sql`
  SQL parsing, binding, statement routing, and `BoundStatement` handoff.
- `brewdb-planner`
  Distributed planning layer. Sits between SQL binding and runtime scheduling.
- `brewdb-runtime`
  Fragment scheduling, transaction coordination, and runtime metadata integration.
- `brewdb-execution`
  DataFusion-aligned fragment execution and exchange runtime. Arrow is the in-memory execution baseline.
- `brewdb-storage`
  Storage semantics kernel with `StorageEngine / TableEngine` boundaries.

## Core Architecture Decisions

- BrewDB owns its catalog directly; it does not depend on Lakekeeper as an architectural prerequisite.
- Catalog naming is unified as `catalog.database.table`.
- Catalog metadata and runtime metadata are separate logical subsystems, even if both use FoundationDB in Phase 1.
- DataFusion is reused for:
  - SQL parsing/binding bridge
  - logical optimization
  - fragment-local physical planning
  - fragment-local physical optimization
- BrewDB owns:
  - distributed planning
  - distributed CBO
  - distributed runtime scheduling
  - transaction and recovery coordination
- Runtime consumes `DistributedPlan`, not raw SQL statements.
- Execution data stays Arrow-compatible. BrewDB does not define a second private row format.

## Current Repository State

The repository currently keeps:

- architecture and rollout docs under `docs/`
- workspace and crate manifests
- crate and binary directory skeletons

The repository intentionally does not currently keep the previous implementation code. The codebase is being rebuilt from the architecture baseline rather than incrementally patching the old scaffold.

## Important Docs

- [Development Architecture](docs/development-architecture.md)
- [Catalog Model](docs/catalog-model.md)
- [Distributed Execution Phase 1](docs/distributed-execution-phase1.md)
- [Coordinator CBO Optimizer Selection](docs/coordinator-cbo-optimizer-selection.md)
- [Framework Rollout Tasks](docs/framework-rollout-tasks.md)
- [Architecture Constraints](docs/architecture-constraints.md)

## Next Build Order

The current rebuild order is:

1. `brewdb-catalog`
2. `brewdb-sql`
3. `brewdb-planner`
4. `brewdb-storage`
5. `brewdb-runtime`
6. `brewdb-execution`
7. `brewdb-frontend`
8. `brewdb` / `brewdbd`

This order follows one rule: define the metadata and planning truth first, then reconnect runtime and execution on top of it.
