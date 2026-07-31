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

## Immediate Refactoring Implications

The current `CatalogService` skeleton should eventually be reshaped to match this design:

- rename current `resolve_catalog` / `resolve_database` / `resolve_table` methods to `get_*`
- replace `*_ref` reads with `get_*_by_id`
- remove `resolve_storage_binding`
- move write inputs from raw entries to semantic request objects
- keep cache orchestration in the service layer

This refactoring is intentionally a follow-up implementation task, not part of this design document itself.
