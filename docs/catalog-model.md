# BrewDB Catalog Model

This document defines BrewDB's built-in catalog model. It covers catalog ownership, the `catalog.database.table` hierarchy, the core table catalog abstraction, FoundationDB-backed persistence, and the boundary between catalog and storage. It does not define FoundationDB key layouts or format-native metadata internals.

## 1. Scope

BrewDB owns its catalog directly.

The built-in catalog is authoritative for:

- catalog, database, and table identity
- schema and planner-visible table metadata
- table format routing
- metadata mutation entry points
- storage-binding metadata needed to reach table engines

Phase 1 persistent backend choice:

- catalog store backend: FoundationDB
- runtime metadata backend: FoundationDB

Both stores use FoundationDB in Phase 1, but they remain separate in role and keyspace.

## 2. Naming Hierarchy

BrewDB uses one first-class naming hierarchy everywhere above table engines:

- `catalog`
- `database`
- `table`

This hierarchy should remain explicit in the model rather than being carried as loose strings.

### Core path objects

```rust
struct CatalogPath {
    catalog: CatalogName,
}

struct DatabasePath {
    catalog: CatalogName,
    database: DatabaseName,
}

struct TablePath {
    catalog: CatalogName,
    database: DatabaseName,
    table: TableName,
}
```

Rules:

- `TablePath` is the canonical table naming path in BrewDB
- planner, runtime logging, cache keys, and DDL flows should all converge on this hierarchy
- `warehouse` and `namespace` are not first-class catalog concepts in BrewDB's internal model

## 3. Path, Ref, and Entry

Catalog objects should be modeled in three layers:

- `Path`: naming hierarchy
- `Ref`: stable object identity with UUID
- `Entry`: full catalog object state

### Stable object refs

```rust
struct CatalogRef {
    catalog_id: CatalogId,
    path: CatalogPath,
}

struct DatabaseRef {
    database_id: DatabaseId,
    path: DatabasePath,
}

struct TableRef {
    table_id: TableId,
    path: TablePath,
}
```

Rules:

- `Path` answers what the object is called and where it lives
- `Ref` answers which object it is, even if names change later
- UUID-backed refs should be the stable identity used by persistence, cache invalidation, and internal cross-component references

## 4. Name Resolution

SQL may reference tables with incomplete qualification, so unresolved and resolved names must stay separate.

```rust
enum UnresolvedTableRef {
    Table(TableName),
    DatabaseTable {
        database: DatabaseName,
        table: TableName,
    },
    Full(TablePath),
}

struct CatalogResolveContext {
    default_catalog: Option<CatalogName>,
    default_database: Option<DatabaseName>,
}
```

Resolution produces a fully qualified `TablePath`, and then the corresponding table ref and catalog entry.

## 5. Catalog Entries

The catalog should expose one entry type per hierarchy level.

```rust
struct CatalogEntry {
    catalog: CatalogRef,
    properties: CatalogProperties,
    state: CatalogObjectState,
}

struct DatabaseEntry {
    database: DatabaseRef,
    properties: DatabaseProperties,
    state: CatalogObjectState,
}
```

## 6. Core Table Abstraction

The main table-facing abstraction should be one core catalog object rather than many parallel metadata DTOs.

```rust
struct TableCatalogEntry {
    table: TableRef,
    schema: TableSchema,
    format: TableFormat,
    storage: TableStorageSpec,
    properties: TableProperties,
    state: CatalogObjectState,
}
```

Rules:

- `TableCatalogEntry` is the main table object exposed upward by `brewdb-catalog`
- planner, compiler, and storage should depend on `TableCatalogEntry` rather than on many parallel route or envelope objects
- if extra table metadata is needed, it should usually become a field or sub-structure of `TableCatalogEntry`, not a new top-level contract
- table-facing planner and storage flows usually need `TableCatalogEntry`; catalog browsing and DDL flows may also need `CatalogEntry` and `DatabaseEntry`

### Storage binding

```rust
struct TableStorageSpec {
    location: String,
    backend: StorageBackendRef,
    options: TableStorageOptions,
}
```

`TableStorageSpec` describes how the table is reached by table engines. It is not a second naming system.

Catalog exposure rule:

- catalog should expose `TableFormat` as stable routing information
- catalog should not expose format-native snapshot, manifest, or commit metadata models
- concrete format behavior begins in `StorageEngine` and `TableEngine`

## 7. CatalogService Role

`CatalogService` is BrewDB's single catalog access layer.

It is responsible for:

- resolving unresolved names into `TablePath`
- loading `CatalogEntry`
- loading `DatabaseEntry`
- loading `TableCatalogEntry`
- mediating cache versus catalog-store reads
- handling catalog writes for DDL and metadata mutations
- normalizing FoundationDB-backed records into BrewDB catalog objects

It is not responsible for:

- runtime job state
- resource leases
- transaction state
- commit journaling
- format-native snapshot truth
- execution provider construction

### Main interface shape

```rust
trait CatalogService {
    fn get_catalog(
        &self,
        path: &CatalogPath,
    ) -> Result<CatalogEntry, CatalogError>;

    fn get_database(
        &self,
        path: &DatabasePath,
    ) -> Result<DatabaseEntry, CatalogError>;

    fn resolve_table(
        &self,
        name: UnresolvedTableRef,
        ctx: CatalogResolveContext,
    ) -> Result<TableCatalogEntry, CatalogError>;

    fn get_table(
        &self,
        path: &TablePath,
    ) -> Result<TableCatalogEntry, CatalogError>;
}
```

## 8. Read and Write Semantics

### Reads

Reads may use a local cache for:

- catalog metadata
- database metadata
- table entries

The cache is best-effort fresh and never authoritative over the FoundationDB-backed catalog store.

### Writes

Catalog writes must go to the BrewDB catalog store.

Successful writes must synchronously:

- refresh the affected cache entry, or
- invalidate the affected cache entry

BrewDB must not rely on TTL alone to discover metadata changes caused by its own write path.

## 9. Cache Granularity

Phase 1 cache units are:

- catalog
- database
- table

Negative cache entries should be short-lived and conservative, especially for missing tables or databases.

## 10. Planner and Storage Boundary

The compiler and planner should operate on `TableCatalogEntry`, not on raw storage metadata and not on FoundationDB record shapes.

The planner uses:

- `table.path`
- `schema`
- `format`
- planner-visible properties

Storage engines use:

- `format`
- `storage`
- engine-relevant properties

Concrete `TableEngine` implementations own:

- query provider construction
- write and mutation planning
- maintenance semantics
- commit semantics
- snapshot and metadata truth interpretation

## 11. Typical Call Paths

### Query and planning

`Compiler -> CatalogService -> cache or catalog store -> TableCatalogEntry`

### DDL and metadata mutation

`DDL path -> CatalogService -> catalog store write -> cache refresh or invalidate`

### Commit path

`CommitManager -> CatalogService -> TableCatalogEntry lookup -> StorageEngine -> TableEngine`

`CatalogService` provides table identity, `TableFormat`, and storage binding. Concrete `TableEngine` implementations own publish semantics.

## 12. Design Rules

1. All BrewDB catalog metadata access goes through `CatalogService`.
2. `catalog.database.table` is the only first-class naming hierarchy in BrewDB's catalog model.
3. Catalog objects should follow the `Path -> Ref -> Entry` layering.
4. UUID-backed refs are the stable object identity layer.
5. `TableCatalogEntry` is the main table abstraction exposed by `brewdb-catalog`.
6. Reads may use cache; writes must go to the BrewDB catalog store.
7. Successful catalog writes must synchronously refresh or invalidate affected cache entries.
8. BrewDB uses normalized catalog objects, not raw FoundationDB record layouts, as planner and runtime inputs.
9. Storage-binding information belongs under `TableCatalogEntry.storage`, not in a second public naming model.
