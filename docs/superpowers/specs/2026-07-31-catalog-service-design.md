# CatalogService Design

Date: 2026-07-31
Branch: `catalog-foundation`

## Scope

This document defines the public service-layer contract for `brewdb-catalog`.

It focuses on:

- the role of `CatalogService`
- read and write interface shape
- request and error modeling
- the boundary between `CatalogService`, `CatalogStore`, `normalize`, and cache

It does not define:

- FoundationDB key layout
- backend record schema
- cache eviction algorithms
- table format internals
- runtime metadata, transaction, or execution concerns

## Goals

- Keep `catalog.database.table` as the only first-class hierarchy exposed upward.
- Make `CatalogService` the only catalog-facing entry point used by planner, SQL, and DDL flows.
- Keep persistence shapes and cache internals inside `brewdb-catalog`.
- Support both path-based lookup and UUID-based stable identity lookup.
- Keep write APIs aligned with user-visible catalog operations instead of low-level store methods.

## Non-Goals

- Building a generic request/response protocol layer inside the catalog crate
- Exposing backend records or cache keys outside the catalog crate
- Letting write APIs infer default catalog or database names

## Service Role

`CatalogService` is the single public access layer for BrewDB catalog metadata.

It is responsible for:

- resolving partially qualified table names into fully qualified `TablePath`
- loading `CatalogEntry`, `DatabaseEntry`, and `TableCatalogEntry`
- mediating cache versus store reads
- executing catalog write operations for DDL and metadata mutation flows
- shaping backend and normalization failures into catalog-facing errors

It is not responsible for:

- runtime job metadata
- transaction state or lock management
- execution scheduling
- storage engine execution
- format-native snapshot truth
- backend key layout or persistence record ownership

## Public Read Interface

The read interface uses object-oriented methods for the three catalog hierarchy levels, plus one explicit SQL-oriented resolution entry point.

```rust
trait CatalogService {
    fn get_catalog(&self, path: &CatalogPath) -> Result<CatalogEntry, CatalogError>;

    fn get_database(&self, path: &DatabasePath) -> Result<DatabaseEntry, CatalogError>;

    fn get_table(&self, path: &TablePath) -> Result<TableCatalogEntry, CatalogError>;

    fn get_catalog_by_id(&self, catalog_id: Uuid) -> Result<CatalogEntry, CatalogError>;

    fn get_database_by_id(&self, database_id: Uuid) -> Result<DatabaseEntry, CatalogError>;

    fn get_table_by_id(&self, table_id: Uuid) -> Result<TableCatalogEntry, CatalogError>;

    fn resolve_table(
        &self,
        name: UnresolvedTableName,
        ctx: &CatalogResolveContext,
    ) -> Result<TableCatalogEntry, CatalogError>;
}
```

Rules:

- `get_*` methods only accept fully qualified paths or stable UUIDs.
- `resolve_table` is the only read entry point that accepts unresolved SQL names.
- `resolve_table` performs default `catalog` and `database` binding through `CatalogResolveContext`.
- `CatalogService` should not expose narrower helper accessors such as `resolve_storage_binding`; callers should consume `TableCatalogEntry`.

## Public Write Interface

The write interface is semantic and request-based.

```rust
trait CatalogService {
    fn create_catalog(
        &self,
        req: CreateCatalogRequest,
    ) -> Result<CatalogEntry, CatalogError>;

    fn create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<DatabaseEntry, CatalogError>;

    fn create_table(
        &self,
        req: CreateTableRequest,
    ) -> Result<TableCatalogEntry, CatalogError>;

    fn alter_table(
        &self,
        req: AlterTableRequest,
    ) -> Result<TableCatalogEntry, CatalogError>;

    fn drop_table(&self, req: DropTableRequest) -> Result<(), CatalogError>;
}
```

Rules:

- Write APIs do not perform default catalog or database inference.
- Requests carry fully qualified paths or stable UUIDs.
- Table mutation breadth is folded into `AlterTableRequest` instead of being split into many ad hoc service methods.
- Future additions such as `alter_database`, `drop_database`, or `drop_catalog` follow the same semantic request pattern.

## Supporting Public Contracts

`CatalogService` depends on a small set of public supporting contracts. These contracts belong to the catalog crate public API because callers need them to express read resolution and write operations, but they should remain narrowly scoped to catalog semantics.

### UnresolvedTableName

`UnresolvedTableName` models SQL-facing input before catalog resolution.

```rust
pub enum UnresolvedTableName {
    Table(String),
    DatabaseTable {
        database: String,
        table: String,
    },
    CatalogDatabaseTable {
        catalog: String,
        database: String,
        table: String,
    },
}
```

Rules:

- it exists only on the path from SQL-facing layers into `CatalogService::resolve_table`
- it does not carry resolution output or catalog object identity
- once resolution succeeds, downstream planning should use `TablePath` and `TableCatalogEntry`

### CatalogResolveContext

`CatalogResolveContext` carries only the information required to bind partially qualified names.

```rust
pub struct CatalogResolveContext {
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
}
```

Rules:

- it is scoped only to unresolved name binding
- it does not carry job, transaction, runtime, or statement execution settings
- future extensions should remain limited to identifier resolution semantics

### Write Request Contracts

Create requests are semantic service-layer requests, not persistence-layer records.

```rust
pub struct CreateCatalogRequest { ... }
pub struct CreateDatabaseRequest { ... }
pub struct CreateTableRequest { ... }
```

Rules:

- each create request identifies a fully qualified target path
- each create request carries enough initial content to materialize the corresponding catalog entry
- create requests do not accept unresolved names

### TableTarget

Table-oriented write operations should support both stable identity and fully qualified path targeting.

```rust
pub enum TableTarget {
    ById(Uuid),
    ByPath(TablePath),
}
```

Rules:

- `ById` is the preferred target shape once stable table identity is already available
- `ByPath` remains useful for DDL-facing flows and diagnostics
- target identity must be explicit in write requests rather than inferred implicitly

### DropTableRequest

`DropTableRequest` should remain minimal.

```rust
pub struct DropTableRequest {
    pub target: TableTarget,
    pub if_exists: bool,
}
```

### AlterTableRequest

`AlterTableRequest` is the single semantic entry point for table-level catalog mutation.

```rust
pub struct AlterTableRequest {
    pub target: TableTarget,
    pub action: AlterTableAction,
}
```

`AlterTableAction` should collect mutation semantics in one place.

```rust
pub enum AlterTableAction {
    Rename {
        new_path: TablePath,
    },
    ReplaceSchema {
        /* deferred */
    },
    SetProperties {
        /* deferred */
    },
    RemoveProperties {
        /* deferred */
    },
    UpdateStorageBinding {
        /* deferred */
    },
    SetState {
        /* deferred */
    },
}
```

Rules:

- `Rename` changes the catalog path but not the stable table id
- schema mutation enters through `ReplaceSchema` first; finer-grained schema mutation can remain an internal expansion later
- property mutation is distinct from storage-binding mutation
- format-native snapshot or manifest mutation is outside the catalog service contract

## Request Model

Each request object carries two kinds of information:

1. Target object identity
2. Operation semantics

### Target identity

- create requests identify the target path being created
- alter and drop requests identify the target by fully qualified path or stable UUID

### Operation semantics

- create requests carry the initial object content needed to materialize the entry
- alter requests carry a mutation descriptor
- drop requests carry drop behavior options

The service-layer contract intentionally does not freeze the internal request fields yet. The important design constraint is that the request boundary is semantic and complete enough that callers do not need to invoke low-level store helpers directly.

## Error Model

`CatalogError` remains the only public error type for the catalog crate, integrated with `brewdb-common` diagnostics and error code infrastructure.

The error model is split into two semantic groups.

### Catalog semantic errors

- `CatalogNotFound`
- `DatabaseNotFound`
- `TableNotFound`
- `DuplicateCatalog`
- `DuplicateDatabase`
- `DuplicateTable`
- `InvalidTableNameResolution`
- `UnsupportedCatalogOperation`
- `ConcurrentCatalogUpdate`

These errors describe user-visible catalog semantics and should carry enough path or identity detail for diagnosis.

### Internal catalog failures

- `Backend`
- `Cache`
- `Normalization`

These errors wrap lower-level failures while preserving structured diagnostics through the common error framework.

Rules:

- public APIs return `CatalogError` only
- callers should be able to distinguish semantic conflicts from internal failures by variant
- the error surface should not introduce a second public `kind` abstraction on top of variants

## Layer Boundaries

The internal layering is strictly one-way:

`CatalogService` -> `CatalogStore` -> backend

and:

`CatalogStore` <-> `normalize`

plus:

`CatalogService` <-> `CatalogCacheManager`

### CatalogService

- public orchestration layer
- performs unresolved name resolution
- shapes user-facing read and write semantics
- controls cache lookups and post-write cache refresh or invalidation

### CatalogStore

- internal repository boundary
- reads and writes catalog objects
- returns normalized BrewDB catalog models
- does not expose backend records upward
- does not own SQL resolution semantics

### normalize

- internal shape-conversion boundary
- translates between backend persistence records and normalized BrewDB catalog objects
- does not own cache policy, retries, or public API semantics

### CatalogCacheManager

- internal cache control plane
- owns invalidation, refresh, capacity, and stats behavior
- only caches normalized catalog objects
- is never authoritative over the store backend

## Read Path

The standard read flow is:

1. `CatalogService` receives `get_*` or `resolve_table`.
2. If the call is `resolve_table`, unresolved SQL naming is converted into a fully qualified `TablePath`.
3. `CatalogService` consults `CatalogCacheManager`.
4. On a cache miss, `CatalogService` calls `CatalogStore`.
5. `CatalogStore` uses backend access and `normalize` to materialize a normalized entry.
6. `CatalogService` triggers cache fill or refresh.
7. The normalized catalog object is returned to the caller.

## Write Path

The standard write flow is:

1. `CatalogService` receives a semantic write request.
2. `CatalogService` enforces catalog-level semantic checks.
3. `CatalogService` calls `CatalogStore`.
4. `CatalogStore` uses `normalize` as needed to persist backend shapes.
5. On success, `CatalogService` refreshes or invalidates affected cache entries.
6. The latest normalized entry, or `()`, is returned.

Rules:

- `CatalogService` must not mutate backend records directly.
- `CatalogStore` must not leak persistence shapes into planner or SQL layers.
- `CatalogCacheManager` must not be the source of truth for catalog metadata.

## Integration Notes

- planner and SQL layers should depend on `CatalogService`, not on `CatalogStore`
- planner and storage integration should consume `TableCatalogEntry`
- `CatalogService` should remain the only component that accepts unresolved SQL object names
- later backend-specific work in FoundationDB should stay below the service contract defined here

## Query-Time Table Binding

`CatalogService` does not by itself solve query-time metadata consistency.

For MVCC table formats such as Iceberg and Paimon, worker execution must not re-read the latest table metadata from the live catalog once a query has already been planned. Otherwise schema, snapshot, or file-set drift may occur between coordinator-side planning and worker-side execution.

BrewDB should therefore separate four layers of table state:

```rust
TableCatalogEntry -> BoundTableSnapshot -> BoundTableScan -> TableSplit
```

### TableCatalogEntry

- catalog-owned table definition
- answers which table the query targets
- may evolve over time as DDL or metadata mutation happens

### BoundTableSnapshot

- query-scoped immutable binding
- freezes the table version selected during planning
- should carry stable execution identity such as table id, table path, format, frozen schema, and snapshot or version identifier

### BoundTableScan

- query-scoped scan plan for a specific `BoundTableSnapshot`
- carries scan-level execution inputs such as projection, pushed predicates, pruning results, and scan options
- is still higher level than worker-local splits

### TableSplit

- worker-consumable unit of scan work
- references a subset of data files, row groups, partitions, or format-native scan tasks derived from the bound scan

Rules:

- the query-ingress node uses `CatalogService` to resolve names and load `TableCatalogEntry`
- planning and binding freeze that metadata into `BoundTableSnapshot`
- scan planning derives `BoundTableScan`
- split planning derives `TableSplit`
- distributed execution consumes `BoundTableSnapshot`, `BoundTableScan`, or `TableSplit`, not a fresh live catalog lookup by table name
- worker execution must not read the latest catalog state to decide what snapshot or schema to scan
- `CatalogService` remains node-local and role-agnostic; every `brewdbd` node constructs it at bootstrap, but query execution uses frozen bindings instead of re-resolving latest table metadata during scan execution

## Immediate Refactoring Implications

The current `CatalogService` skeleton should eventually be reshaped to match this design:

- rename current `resolve_catalog` / `resolve_database` / `resolve_table` methods to `get_*`
- replace `*_ref` reads with `get_*_by_id`
- remove `resolve_storage_binding`
- move write inputs from raw entries to semantic request objects
- keep cache orchestration in the service layer

This refactoring is intentionally a follow-up implementation task, not part of this design document itself.
