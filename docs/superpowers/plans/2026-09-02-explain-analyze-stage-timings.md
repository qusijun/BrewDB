# Explain Analyze Stage Timings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface parser, planner, catalog, and execution timing in `EXPLAIN ANALYZE`.

**Architecture:** Keep timing collection at the existing query boundary layers instead of threading a new tracing object through the whole stack. Record parser/planner/catalog phase durations in the driver and coordinator, then attach them to the existing shared `QueryProfile` model so `EXPLAIN ANALYZE` and JSON profiles can render them consistently.

**Tech Stack:** Rust, `QueryProfiler`, `QueryProfile`, DataFusion logical/physical planning, sqllogictest.

---

### Task 1: Add query phase timing capture at the SQL driver boundary

**Files:**
- Modify: `crates/brewdb-execution/src/runtime/driver.rs`
- Modify: `crates/brewdb-execution/src/runtime/profile.rs`
- Test: `crates/brewdb-execution/src/runtime/driver.rs` tests

- [ ] **Step 1: Write the failing test**

Add a driver test that executes a trivial query and asserts the emitted `QueryProfile` contains phase entries for parser and planner, with nonzero elapsed times:

```rust
#[test]
fn sql_driver_records_parser_and_planner_phases() {
    let catalog_service = catalog_service(warehouse.path());
    let driver = SqlDriver::new(catalog_service, QueryCoordinator::default());
    let handle = driver.execute(
        "select 1",
        QueryContext::for_test(uuid::Uuid::new_v4()),
    ).unwrap();
    let profile = handle.output.profile().unwrap();
    assert!(profile.phases.iter().any(|phase| phase.name == "parse"));
    assert!(profile.phases.iter().any(|phase| phase.name == "plan"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p brewdb-execution sql_driver_records_parser_and_planner_phases -- --exact`

Expected: FAIL because the profile does not yet contain parse/plan phases.

- [ ] **Step 3: Write minimal implementation**

Wrap SQL parsing and logical planning in scoped `QueryProfiler` phases in `SqlDriver::execute`, and store the resulting `QueryProfile` in the query handle/output path. Use explicit phase names:

```rust
let _parse_phase = profiler.scoped_phase("parse");
let statements = Parser::parse_sql(&dialect, sql)?;

let _plan_phase = profiler.scoped_phase("plan");
let logical_plan = self.logical_planner.plan(statement, &planning_context)?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p brewdb-execution sql_driver_records_parser_and_planner_phases -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/brewdb-execution/src/runtime/driver.rs crates/brewdb-execution/src/runtime/profile.rs
git commit -m "Record parser and planner timings"
```

### Task 2: Record catalog resolution time separately from planning

**Files:**
- Modify: `crates/brewdb-execution/src/runtime/driver.rs`
- Modify: `crates/brewdb-catalog/src/service.rs`
- Test: `crates/brewdb-execution/src/runtime/driver.rs` tests

- [ ] **Step 1: Write the failing test**

Add a driver test for a catalog lookup path, such as planning a `select * from prod.sales.orders`, and assert the profile contains a `catalog` phase:

```rust
#[test]
fn sql_driver_records_catalog_phase_for_table_queries() {
    let profile = execute_sql_and_get_profile("select * from prod.sales.orders");
    assert!(profile.phases.iter().any(|phase| phase.name == "catalog"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p brewdb-execution sql_driver_records_catalog_phase_for_table_queries -- --exact`

Expected: FAIL because catalog work is not yet measured as its own phase.

- [ ] **Step 3: Write minimal implementation**

Time the catalog resolution segment inside planning, covering table and database lookup that happens before logical planning chooses scan bindings:

```rust
let _catalog_phase = profiler.scoped_phase("catalog");
let table_catalogs = self.catalog_service.lookup_tables(...)?;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p brewdb-execution sql_driver_records_catalog_phase_for_table_queries -- --exact`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/brewdb-execution/src/runtime/driver.rs crates/brewdb-catalog/src/service.rs
git commit -m "Measure catalog lookup timing"
```

### Task 3: Expose phase timings in EXPLAIN ANALYZE output

**Files:**
- Modify: `crates/brewdb-common/src/profile.rs`
- Modify: `crates/brewdb-execution/src/runtime/profile.rs`
- Modify: `crates/brewdb-planner/src/logical/explain.rs`
- Modify: `crates/brewdb-sqllogictests/test_files/explain.slt`
- Test: `crates/brewdb-common/src/profile.rs` tests, `crates/brewdb-sqllogictests/test_files/explain.slt`

- [ ] **Step 1: Write the failing test**

Add a profile serialization test that expects phase names to survive JSON serialization, and add a sqllogictest case asserting `EXPLAIN ANALYZE` prints the new parser/planner/catalog phase labels.

```rust
#[test]
fn query_profile_serializes_phase_names() {
    let profile = QueryProfile {
        phases: vec![PhaseProfile { name: "parse".to_owned(), elapsed_ms: 1 }],
        ..sample_profile()
    };
    let json = profile.to_json_string().unwrap();
    assert!(json.contains("\"parse\""));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p brewdb-common query_profile_serializes_phase_names -- --exact`

Expected: FAIL if phase fields are missing or renamed.

- [ ] **Step 3: Write minimal implementation**

Keep phase data on `QueryProfile` and extend the explain analyzer to render phase timing alongside the existing plan-with-metrics output. Use the existing profile model rather than introducing a separate explain-only DTO.

- [ ] **Step 4: Run test to verify it passes**

Run:
- `cargo test -p brewdb-common query_profile_serializes_phase_names -- --exact`
- `cargo test -p brewdb-sqllogictests --test <appropriate harness>`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/brewdb-common/src/profile.rs crates/brewdb-execution/src/runtime/profile.rs crates/brewdb-planner/src/logical/explain.rs crates/brewdb-sqllogictests/test_files/explain.slt
git commit -m "Surface stage timings in explain analyze"
```

### Task 4: Validate end-to-end explain analyze output

**Files:**
- Modify: `crates/brewdb-sqllogictests/test_files/explain.slt`
- Test: `crates/brewdb-sqllogictests`

- [ ] **Step 1: Write the failing test**

Add one sqllogictest case that runs `explain analyze` on a query using a table lookup and asserts the output includes parser, planner, catalog, and execution timing labels.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p brewdb-sqllogictests`

Expected: FAIL until the explain output is updated.

- [ ] **Step 3: Write minimal implementation**

Adjust the explain formatting path only enough to include the new timings without changing the existing physical plan tree layout more than necessary.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p brewdb-sqllogictests`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/brewdb-sqllogictests/test_files/explain.slt
git commit -m "Cover explain analyze stage timings"
```

### Task 5: Full verification

**Files:**
- N/A

- [ ] **Step 1: Run formatting and checks**

Run:
```bash
cargo fmt --all
cargo check
cargo test -p brewdb-execution
cargo test -p brewdb-common
cargo test -p brewdb-sqllogictests
```

- [ ] **Step 2: Confirm output**

Expected: all commands pass, and `EXPLAIN ANALYZE` includes stage timing for parser, planner, catalog, and execution.

- [ ] **Step 3: Commit final state**

```bash
git add -A
git commit -m "Wire stage timings into explain analyze"
```
