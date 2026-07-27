# BrewDB Catalog Model

This document defines BrewDB's catalog model around Lakekeeper. It covers control-plane boundaries, local cache behavior, table metadata envelopes, and multi-format routing. It does not define Lakekeeper internals.

## 1. Scope

BrewDB treats Lakekeeper as the authoritative control plane for:

- namespace and table identity
- authz anchors
- warehouse and storage profile binding
- credential vending entry points
- format routing
- metadata mutation entry points

BrewDB may cache catalog reads locally, but Lakekeeper remains the source of truth.

## 2. CatalogFacade Role

`CatalogFacade` is BrewDB's single control-plane access layer.

It is responsible for:

- namespace and table resolution
- control-plane metadata reads
- local catalog cache mediation
- Lakekeeper-to-BrewDB metadata normalization
- control-plane write entry for DDL and metadata mutations

It is not responsible for:

- runtime job state
- resource leases
- transaction state
- commit journaling
- format-specific snapshot truth

## 3. Read and Write Semantics

### Reads

Reads may use a local cache for:

- namespace/database metadata
- table metadata
- warehouse/storage profile metadata

The cache is best-effort fresh and never authoritative over Lakekeeper.

### Writes

Metadata writes must go to Lakekeeper.

Successful writes must synchronously:

- refresh the affected cache entry, or
- invalidate the affected cache entry

BrewDB must not rely on TTL alone to discover metadata changes caused by its own write path.

## 4. Cache Granularity

Phase 1 cache units are:

- namespace/database
- table metadata
- warehouse/storage profile

Negative cache entries should be short-lived and conservative, especially for missing tables or namespaces.

## 5. Internal Table Metadata Envelope

BrewDB should not directly expose raw Lakekeeper response objects to planners or adapters.

Instead, it should normalize catalog objects into a Lakekeeper-aligned internal envelope with four parts:

- identity
- control-plane view
- planner view
- format handle

### Identity

- `table_id`
- `namespace_id`
- `namespace_name`
- `table_name`
- `format_type`
- `warehouse_id`

### Control-plane view

- `revision` or `etag`
- `properties`
- `authz_anchor`
- `catalog_binding`
- `lifecycle_state`

### Planner view

- `normalized_schema`
- `partitioning_spec` when applicable
- `sort_or_clustering_spec` when applicable
- `table_capabilities`

### Format handle

- `format_type`
- `catalog_object_ref`
- `table_location`
- `format_revision` when available

The envelope should hold a format handle, not format truth.

## 6. Multi-Format Model

BrewDB uses a two-level catalog abstraction:

- a unified catalog shell for identity and control-plane access
- a format-specific handle for adapter routing

### Namespace model

Namespaces are control-plane objects and remain format-agnostic.

### Table model

Tables carry:

- `format_type`
- `catalog_binding`
- `adapter_key`

This is the table's format route.

## 7. Planner and Adapter Boundary

The planner should operate on the normalized envelope, not on raw format metadata.

The planner uses:

- normalized schema
- table capabilities
- partitioning or clustering hints
- format route

Format adapters use:

- the format handle
- control-plane route information

Format adapters own:

- scan binding
- write and mutation planning
- maintenance semantics
- commit semantics
- snapshot and metadata truth interpretation

## 8. Capability Model

Capability checks should be unified at the BrewDB layer, but sourced from format-aware metadata and adapters.

Examples:

- insert support
- delete support
- merge support
- compact support
- schema evolution support

The planner consumes capabilities uniformly even though capability definitions remain format-sensitive.

## 9. Typical Call Paths

### Query and planning

`Planner -> CatalogFacade -> cache or Lakekeeper -> normalized envelope`

### DDL and metadata mutation

`DDL path -> CatalogFacade -> Lakekeeper write -> cache refresh/invalidate`

### Commit path

`CommitManager -> CatalogFacade -> route/handle lookup -> format adapter`

`CatalogFacade` provides the route and handle. Format adapters own the actual publish semantics.

## 10. Design Rules

1. All BrewDB control-plane metadata access goes through `CatalogFacade`.
2. Reads may use cache; writes must go to Lakekeeper.
3. Successful metadata writes must synchronously refresh or invalidate affected cache entries.
4. BrewDB uses normalized metadata envelopes, not raw Lakekeeper response objects, as internal planner inputs.
5. Namespaces are format-agnostic; tables carry format routing.
6. Capability checks are unified; format semantics are not.
