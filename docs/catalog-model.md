# BrewDB Catalog Model

This document defines BrewDB's built-in catalog model. It covers catalog ownership, the `catalog.database.table` hierarchy, the core table catalog abstraction, FoundationDB-backed persistence, and the boundary between catalog and storage. It does not define FoundationDB key layouts or format-native metadata internals.

## 1. Scope

BrewDB owns its catalog directly.

The built-in catalog is authoritative for:

- catalog, database, and table identity
- table format routing
- metadata mutation entry points
- stable storage-binding metadata needed to reach table engines

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
- UUID-backed refs should be the stable identity used by persistence and internal cross-component references

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
- `TableCatalogEntry` does not duplicate format-native schema or snapshot truth inside BrewDB's catalog store

### Storage binding

```rust
struct TableStorageSpec {
    location: String,
    backend: StorageBackendRef,
    options: TableStorageOptions,
}
```

`TableStorageSpec` describes how the table is reached by table engines. It is not a second naming system, and it should remain stable across snapshot churn.

Catalog exposure rule:

- catalog should expose `TableFormat` as stable routing information
- catalog should not expose format-native snapshot, manifest, or commit metadata models
- catalog should not publish latest-snapshot pointers for Paimon-managed tables
- concrete format behavior begins in `StorageEngine` and `TableEngine`

## 7. CatalogService Role

`CatalogService` is BrewDB's single catalog access layer.

It is responsible for:

- opening or looking up `Catalog` instances by catalog name
- resolving unresolved names into `TablePath`
- loading `CatalogEntry`
- loading `DatabaseEntry`
- loading `TableCatalogEntry`
- opening the owning `Catalog` and routing table-level metadata resolution to it
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
}
```

Table schema resolution belongs inside `Catalog`, not in `CatalogStore` and not as a separate top-level resolver service. `CatalogService` should stay at the catalog lookup boundary; table lookup and schema lookup happen on the opened catalog instance.

## 8. Read and Write Semantics

### Reads

Reads go through `CatalogService` to the registered `Catalog` implementation and the FoundationDB-backed catalog store. Phase 1 does not rely on a per-node metadata cache in the main catalog path.

### Writes

Catalog writes must go through the owning `Catalog` implementation and persist BrewDB's control-plane metadata in the catalog store.

For managed Paimon catalogs, the stored table metadata is intentionally narrow:

- stable table identity
- `catalog.database.table`
- lake format kind
- table root location

Schema, snapshot selection, manifest resolution, and file enumeration remain format-native responsibilities below the catalog boundary.

## 9. Planner and Storage Boundary

The compiler and planner should operate on `TableCatalogEntry`, not on raw storage metadata and not on FoundationDB record shapes.

The planner uses:

- `table.path`
- `format`
- control-plane table identity
- later, query-scoped schema resolved from the underlying table format

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

## 10. Typical Call Paths

### Query and planning

`Compiler -> CatalogService -> catalog store / catalog implementation -> TableCatalogEntry`

### DDL and metadata mutation

`DDL path -> CatalogService -> Catalog implementation -> catalog store write`

### Commit path

`CommitManager -> CatalogService -> TableCatalogEntry lookup -> StorageEngine -> TableEngine`

`CatalogService` provides table identity, `TableFormat`, and storage binding. Concrete `TableEngine` implementations own publish semantics.

## 11. Design Rules

1. All BrewDB catalog metadata access goes through `CatalogService`.
2. `catalog.database.table` is the only first-class naming hierarchy in BrewDB's catalog model.
3. Catalog objects should follow the `Path -> Ref -> Entry` layering.
4. UUID-backed refs are the stable object identity layer.
5. `TableCatalogEntry` is the main table abstraction exposed by `brewdb-catalog`.
6. BrewDB uses normalized catalog objects, not raw FoundationDB record layouts, as planner and runtime inputs.
7. `TableCatalogEntry` stores stable table location routing, not format-native schema or snapshot truth.
8. Storage-binding information belongs under `TableCatalogEntry.storage`, not in a second public naming model.
