# BrewDB Catalog Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `brewdb-catalog` public service contract so the crate exposes a trait-based `CatalogService`, semantic request/resolve contracts, and a default implementation aligned with the approved catalog design spec.

**Architecture:** Keep `CatalogService` as the only public catalog entry point. Add supporting public contracts for unresolved names and write requests, convert the current concrete service into a `DefaultCatalogService` that implements a `CatalogService` trait, and narrow crate exports so store/cache/backend stay internal-facing details instead of the main public API.

**Tech Stack:** Rust, `uuid`, existing `brewdb-common` diagnostics/error framework, existing `brewdb-catalog` memory/FDB store skeleton

---

## File Map

- Modify: `crates/brewdb-catalog/src/lib.rs`
  Responsibility: narrow crate public surface and export the new service contracts.
- Modify: `crates/brewdb-catalog/src/errors.rs`
  Responsibility: add service-contract errors required by the approved design and preserve diagnostics integration.
- Modify: `crates/brewdb-catalog/src/model.rs`
  Responsibility: keep catalog objects aligned with the new service trait tests where needed.
- Modify: `crates/brewdb-catalog/src/service.rs`
  Responsibility: define the `CatalogService` trait, `DefaultCatalogService`, unresolved-name and write-request contracts, and implement the read/write orchestration.
- Test: `crates/brewdb-catalog/src/service.rs`
  Responsibility: unit tests for the trait-facing semantics and default implementation behavior.
- Test: `crates/brewdb-catalog/src/errors.rs`
  Responsibility: unit tests for new error variants and diagnostics mapping.

### Task 1: Add failing tests for the public catalog service contract

**Files:**
- Modify: `crates/brewdb-catalog/src/service.rs`
- Test: `crates/brewdb-catalog/src/service.rs`

- [ ] **Step 1: Write the failing tests for unresolved-name resolution and write-request semantics**

```rust
    #[test]
    fn resolve_table_binds_default_catalog_and_database() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        service
            .create_table(CreateTableRequest::new(
                Uuid::new_v4(),
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let resolved = service
            .resolve_table(
                UnresolvedTableName::Table("orders".to_owned()),
                &CatalogResolveContext::new(Some("prod"), Some("sales")),
            )
            .unwrap();

        assert_eq!(resolved.path.to_string(), "prod.sales.orders");
    }

    #[test]
    fn get_table_by_id_returns_table_entry() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();
        service
            .create_database(CreateDatabaseRequest::new(
                Uuid::new_v4(),
                DatabasePath::new("prod", "sales").unwrap(),
            ))
            .unwrap();
        let table_id = Uuid::new_v4();
        service
            .create_table(CreateTableRequest::new(
                table_id,
                TablePath::new("prod", "sales", "orders").unwrap(),
                StorageBinding::new(TableFormat::Paimon, "s3://warehouse/orders"),
            ))
            .unwrap();

        let table = service.get_table_by_id(table_id).unwrap();

        assert_eq!(table.table_id, table_id);
    }

    #[test]
    fn drop_table_request_supports_if_exists() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service = DefaultCatalogService::new(CatalogStore::new(backend));

        let result = service.drop_table(DropTableRequest::new(
            TableTarget::ByPath(TablePath::new("prod", "sales", "missing").unwrap()),
            true,
        ));

        assert!(result.is_ok());
    }
```

- [ ] **Step 2: Run the targeted tests to confirm they fail**

Run: `cargo test -p brewdb-catalog service::tests::resolve_table_binds_default_catalog_and_database service::tests::get_table_by_id_returns_table_entry service::tests::drop_table_request_supports_if_exists --offline`

Expected: FAIL with missing types such as `DefaultCatalogService`, `CreateTableRequest`, `DropTableRequest`, or missing trait methods.

- [ ] **Step 3: Commit the failing-test checkpoint**

```bash
git add crates/brewdb-catalog/src/service.rs
git commit -m "test: add catalog service contract coverage"
```

### Task 2: Implement supporting public contracts and error variants

**Files:**
- Modify: `crates/brewdb-catalog/src/errors.rs`
- Modify: `crates/brewdb-catalog/src/service.rs`
- Test: `crates/brewdb-catalog/src/errors.rs`

- [ ] **Step 1: Add failing tests for the new error surface**

```rust
    #[test]
    fn invalid_table_name_resolution_maps_to_invalid_configuration() {
        let error = CatalogError::InvalidTableNameResolution {
            name: "orders".to_owned(),
            reason: "default database is not set".to_owned(),
        };

        assert_eq!(error.error_code(), ErrorCode::InvalidConfiguration);
        assert_eq!(error.variant_name(), "InvalidTableNameResolution");
    }

    #[test]
    fn cache_and_normalization_errors_map_to_internal() {
        let cache = CatalogError::Cache {
            message: "cache refresh failed".to_owned(),
        };
        let normalization = CatalogError::Normalization {
            message: "missing table_id".to_owned(),
        };

        assert_eq!(cache.error_code(), ErrorCode::Internal);
        assert_eq!(normalization.error_code(), ErrorCode::Internal);
    }
```

- [ ] **Step 2: Run the error tests to confirm they fail**

Run: `cargo test -p brewdb-catalog errors::tests::invalid_table_name_resolution_maps_to_invalid_configuration errors::tests::cache_and_normalization_errors_map_to_internal --offline`

Expected: FAIL because the new variants do not exist yet.

- [ ] **Step 3: Implement the minimal supporting contracts and errors**

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogResolveContext {
    pub default_catalog: Option<String>,
    pub default_database: Option<String>,
}

impl CatalogResolveContext {
    pub fn new(
        default_catalog: Option<impl Into<String>>,
        default_database: Option<impl Into<String>>,
    ) -> Self {
        Self {
            default_catalog: default_catalog.map(Into::into),
            default_database: default_database.map(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnresolvedTableName {
    Table(String),
    DatabaseTable { database: String, table: String },
    CatalogDatabaseTable {
        catalog: String,
        database: String,
        table: String,
    },
}
```

```rust
pub enum CatalogError {
    // existing variants...
    InvalidTableNameResolution { name: String, reason: String },
    UnsupportedCatalogOperation { operation: &'static str },
    ConcurrentCatalogUpdate { object: String },
    Backend { message: String },
    Cache { message: String },
    Normalization { message: String },
}
```

- [ ] **Step 4: Run the error tests to verify they pass**

Run: `cargo test -p brewdb-catalog errors::tests::invalid_table_name_resolution_maps_to_invalid_configuration errors::tests::cache_and_normalization_errors_map_to_internal --offline`

Expected: PASS

- [ ] **Step 5: Commit the contract-and-error slice**

```bash
git add crates/brewdb-catalog/src/errors.rs crates/brewdb-catalog/src/service.rs
git commit -m "feat: add catalog service contracts and errors"
```

### Task 3: Convert the concrete service into a trait plus default implementation

**Files:**
- Modify: `crates/brewdb-catalog/src/service.rs`
- Test: `crates/brewdb-catalog/src/service.rs`

- [ ] **Step 1: Add a failing test for the public trait-backed implementation**

```rust
    #[test]
    fn catalog_service_trait_object_uses_default_implementation() {
        let backend = Arc::new(MemoryCatalogStoreBackend::default());
        let service: Arc<dyn CatalogService> =
            Arc::new(DefaultCatalogService::new(CatalogStore::new(backend)));

        service
            .create_catalog(CreateCatalogRequest::new(
                Uuid::new_v4(),
                CatalogPath::new("prod").unwrap(),
            ))
            .unwrap();

        let catalog = service.get_catalog(&CatalogPath::new("prod").unwrap()).unwrap();

        assert_eq!(catalog.path.to_string(), "prod");
    }
```

- [ ] **Step 2: Run the trait-object test to confirm it fails**

Run: `cargo test -p brewdb-catalog service::tests::catalog_service_trait_object_uses_default_implementation --offline`

Expected: FAIL because `CatalogService` is currently a concrete struct, not a trait.

- [ ] **Step 3: Implement the trait and default service**

```rust
pub trait CatalogService: Send + Sync {
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
    fn create_catalog(&self, req: CreateCatalogRequest) -> Result<CatalogEntry, CatalogError>;
    fn create_database(&self, req: CreateDatabaseRequest) -> Result<DatabaseEntry, CatalogError>;
    fn create_table(&self, req: CreateTableRequest) -> Result<TableCatalogEntry, CatalogError>;
    fn alter_table(&self, req: AlterTableRequest) -> Result<TableCatalogEntry, CatalogError>;
    fn drop_table(&self, req: DropTableRequest) -> Result<(), CatalogError>;
}

#[derive(Clone)]
pub struct DefaultCatalogService {
    store: CatalogStore,
    cache_manager: Arc<dyn CatalogCacheManager>,
}
```

- [ ] **Step 4: Implement read/write orchestration with the existing store**

```rust
impl CatalogService for DefaultCatalogService {
    fn get_table_by_id(&self, table_id: Uuid) -> Result<TableCatalogEntry, CatalogError> {
        self.store
            .get_table_by_ref(TableRef::new(table_id))?
            .ok_or_else(|| CatalogError::TableNotFoundById { table_id })
    }

    fn resolve_table(
        &self,
        name: UnresolvedTableName,
        ctx: &CatalogResolveContext,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let path = self.resolve_table_path(name, ctx)?;
        self.get_table(&path)
    }

    fn drop_table(&self, req: DropTableRequest) -> Result<(), CatalogError> {
        if req.if_exists && self.lookup_table_target(&req.target)?.is_none() {
            return Ok(());
        }
        Err(CatalogError::UnsupportedCatalogOperation {
            operation: "drop_table",
        })
    }
}
```

- [ ] **Step 5: Run the service tests to verify the trait-backed implementation passes**

Run: `cargo test -p brewdb-catalog service::tests --offline`

Expected: PASS for the new contract tests and the updated existing service tests.

- [ ] **Step 6: Commit the trait-conversion slice**

```bash
git add crates/brewdb-catalog/src/service.rs
git commit -m "feat: implement default catalog service"
```

### Task 4: Narrow crate exports and preserve the approved public surface

**Files:**
- Modify: `crates/brewdb-catalog/src/lib.rs`
- Test: `crates/brewdb-catalog/src/service.rs`

- [ ] **Step 1: Add a failing test that imports the new public surface from the crate root**

```rust
    #[test]
    fn crate_root_exports_catalog_service_contracts() {
        let _ = brewdb_catalog::CatalogResolveContext::new(Some("prod"), Some("sales"));
        let _ = brewdb_catalog::UnresolvedTableName::Table("orders".to_owned());
        let _ = brewdb_catalog::TableTarget::ById(Uuid::nil());
    }
```

- [ ] **Step 2: Run the targeted test to confirm it fails**

Run: `cargo test -p brewdb-catalog crate_root_exports_catalog_service_contracts --offline`

Expected: FAIL because the crate root does not yet re-export the new contracts.

- [ ] **Step 3: Narrow and reshape the crate-root exports**

```rust
pub use errors::CatalogError;
pub use model::{
    CatalogEntry, CatalogRef, DatabaseEntry, DatabaseRef, StorageBinding, TableCatalogEntry,
    TableFormat, TableRef,
};
pub use path::{CatalogPath, DatabasePath, TablePath};
pub use service::{
    AlterTableAction, AlterTableRequest, CatalogResolveContext, CatalogService,
    CreateCatalogRequest, CreateDatabaseRequest, CreateTableRequest, DefaultCatalogService,
    DropTableRequest, TableTarget, UnresolvedTableName,
};
```

- [ ] **Step 4: Run the full crate test suite**

Run: `cargo test -p brewdb-catalog --offline`

Expected: PASS

- [ ] **Step 5: Commit the public-surface cleanup**

```bash
git add crates/brewdb-catalog/src/lib.rs crates/brewdb-catalog/src/service.rs
git commit -m "refactor: narrow catalog crate public surface"
```

### Task 5: Final verification

**Files:**
- Verify only

- [ ] **Step 1: Run common + catalog verification**

Run: `cargo test -p brewdb-common -p brewdb-catalog --offline`

Expected: PASS

- [ ] **Step 2: Run workspace verification**

Run: `cargo test --offline`

Expected: PASS

- [ ] **Step 3: Review git diff and status**

Run: `git status --short && git log --oneline -5`

Expected: only the planned catalog changes are present, with clear incremental commits.

- [ ] **Step 4: Commit any remaining fixups if needed**

```bash
git add crates/brewdb-catalog/src/lib.rs crates/brewdb-catalog/src/errors.rs crates/brewdb-catalog/src/service.rs
git commit -m "test: finalize catalog service verification"
```
